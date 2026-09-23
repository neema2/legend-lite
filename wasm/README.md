# legend-lite's planner, compiled to WebAssembly

`planner.Wasm` — and everything it reaches in `//core` — compiled ahead of
time by TeaVM to a WebAssembly-GC module, and held to the JVM by
differentials that run on every push. DataCube's `src/wasm-planner.ts`
plans in the browser on this module, with no server.

```bash
bazel build //wasm:planner      # → bazel-bin/wasm/planner/classes.wasm (+ its runtime js)
bazel test  //wasm:all          # the differentials
bazel run   //wasm:startup      # where cold start goes (times things: idle machine only)
```

## The targets

| target | what it is |
|---|---|
| `:boundary` | `Wasm` + `PreludeResources`: the one class both backends run |
| `:planner` | `teavm_wasm` (//tools/teavm): TeaVM over `:boundary`'s jars, one build action → `classes.wasm` and `wasm-gc-module-runtime.js` |
| `:jvm_answers` | `JvmMain` over `corpus/` — the JVM's answers, a build output |
| `:differential_test` | plans `corpus/` in the module and compares with `:jvm_answers`, refusals included |
| `:zone_jvm`, `:zone_test` | the same for `LiteralSpelling.inZone`: is the tz database in the module |
| `:startup` | `bazel run`: cold start, phase by phase |

TeaVM and the class library it compiles against come from `@maven_teavm`
(MODULE.bazel), build tooling that never reaches a running class path.

## Why it is shaped this way

**One source, two backends.** `planner.Wasm` is the entry point for both —
TeaVM compiles it, and `planner.JvmMain` calls the very same class on the
JVM. The differential compares two builds of one source. Two hand-kept
copies would drift apart silently, and a differential that drifts proves
nothing.

**`plan` is `Compiler.plan` and nothing else.** `POST /engine/plan` calls
the same method, so the browser plane and the server plane cannot disagree
about dialect, null ordering or aggregates.

**Failure is a return value, not a throw.** `planOrError` returns
`"OK\n" + sql` or `"ERR\n" + class + "\n" + message`. Refusals are answers
the planner is expected to give, so they are compared too — through the
return value, which avoids depending on how TeaVM bridges Java throwables
into JS.

**`PreludeResources` is packaging, not product.** `Prelude` reads its
source with `getResourceAsStream`; a WASM module has no class path.
TeaVM's build-time `ResourceSupplier` SPI embeds the file instead. It
costs ~300 KB of module.

**The JVM's answers are a build output.** `JvmMain` runs as an action over
`corpus/`; Bazel caches its answers until the planner or the corpus
changes. It times nothing: an action shares its machine with every other
action, so a latency measured there is noise presented as a number.

## What stops this from rotting

The real compile. `//wasm:planner` builds in the gate chain, so an API the
AOT class library lacks fails the build that introduces it:
`java.security.MessageDigest` (hence `core/cache/Sha256`), `String.lines()`
(hence `ElementParser.linesOf`), `ConcurrentHashMap.newKeySet()`, the
StringBuilder overloads of `Matcher.appendReplacement`,
`Thread.dumpStack()`, `java.util.HexFormat`. Checked 2026-09-23 by
reverting `Hash` to `MessageDigest` (TeaVM: `Class
java.security.MessageDigest was not found`) and, separately, to
`HexFormat` (`Class java.util.HexFormat was not found`); both fail the
build. This replaced a source-scanning test that banned the first five by
regex; the compiler knows the whole list, and the regex knew five —
`HexFormat` was not among them.

The differentials then check the outcome, not just the build: 69 queries
(63 planned, 6 refused) byte-identical to the JVM on main as of
2026-09-23, and DataCube's own serialised cube shapes in
`//datacube:wasm_differential_test`.

## Results

Measured 2026-09-20 on the spike (Node 22.16, Apple Silicon, a machine
that was never fully idle — quote the ratio, not the milliseconds; the
full write-up is `research/HANDOFF-2026-09-20.md` §10 on the archived
`datacube/dual-plane` branch):

| | JVM | WASM |
|---|---|---|
| warm p50 | 2.36 - 3.05 ms | 9.57 - 13.92 ms |
| cold (first plan) | 329 - 481 ms | 557 - 771 ms |
| payload | — | 4.2 MB raw / 1.46 MB gzip |

WASM runs the planner **4.0-4.6x slower** than the JVM; that ratio held
across every run regardless of load. duckdb-wasm, already shipped by the
product, is 34 MB, so the planner adds roughly 4% to an existing payload.

## Gotchas worth keeping

- `--experimental-wasm-exnref` is **required on Node 22** (the BUILD
  passes it): TeaVM emits the newer exception-handling opcodes, and
  without the flag the module does not even compile: `Invalid opcode
  0x1f`. Browsers that have shipped WASM-GC need no flag.
- TeaVM's loader wants a URL in a browser and a FILESYSTEM PATH under
  Node. Handing Node a `file:` URL fails with an ENOENT that surfaces as
  "could not instantiate" and reads like a missing WASM-GC feature;
  `WasmPlanner` converts, and says so.
- `ServiceLoader` returns empty in WASM. Built-in section grammars are
  hardcoded and unaffected, but grammar *extensions* can never load in a
  WASM build.
