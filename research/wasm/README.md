# legend-lite's planner, compiled to WebAssembly

A research spike, **not shipped code** and deliberately not a module of
the root reactor. It answers one question with measurements instead of
argument: *can the planner run in a browser, and does it still give the
same answers?*

**It can, and it does.** 69/69 queries byte-identical to the JVM,
refusals included.

## Run it

```bash
# from the repo root — the spike depends on the INSTALLED core jar
mvn -o -pl core -DskipTests install

cd research/wasm
mvn -o package                                   # → target/wasm/classes.wasm
node --experimental-wasm-exnref differential.mjs # 69-query differential
```

`--experimental-wasm-exnref` is **required on Node 22**. TeaVM emits
the newer exception-handling opcodes, and without the flag the module
does not even compile: `Invalid opcode 0x1f`. Browsers that have
shipped WASM-GC need no flag.

Override `JAVA_HOME` and `LEGEND_CORE_JAR` if yours are elsewhere.

The timezone probe is separate because it asks about a *resource*
rather than about code:

```bash
node --experimental-wasm-exnref zoneprobe.mjs > target/zone-wasm.txt
java -cp "$LEGEND_CORE_JAR:target/classes" planner.ZoneMain > target/zone-jvm.txt
diff target/zone-jvm.txt target/zone-wasm.txt
```

## Why it is shaped this way

**One source, two backends.** `planner.Wasm` is the entry point for
both targets — TeaVM compiles it, and `planner.JvmMain` calls the very
same class on the JVM. The differential therefore compares two builds
of one source. Two hand-kept copies would drift apart silently, and a
differential that drifts proves nothing.

**Failure is a return value, not a throw.** `planOrError` returns
`"OK\n" + sql` or `"ERR\n" + class + "\n" + message`. Refusals are
answers the planner is expected to give, so they have to be compared
too — and comparing them through the return value avoids depending on
how TeaVM bridges Java throwables into JS.

**`PreludeResources` is packaging, not product.** `Prelude` reads its
source with `getResourceAsStream`; a WASM module has no classpath.
TeaVM's build-time `org.teavm.classlib.ResourceSupplier` SPI embeds the
file instead. This lives here rather than in `core/` because nothing
about the planner changes — only how its one data file is delivered.
It costs ~300 KB of module.

## Results

See `research/HANDOFF-2026-09-20.md` §10 for the full write-up: the
numbers, the hazard census re-run against the running module, and what
is still unbuilt.

Headline, Node 22.16 on Apple Silicon, across three runs on a machine
that was never fully idle — quote the ratio, not the milliseconds:

| | JVM | WASM |
|---|---|---|
| warm p50 | 2.36 - 3.05 ms | 9.57 - 13.92 ms |
| cold (first plan) | 329 - 481 ms | 557 - 771 ms |
| payload | — | 4.2 MB raw / 1.46 MB gzip |

WASM runs the planner **4.0-4.6x slower** than the JVM; that ratio held
across every run regardless of load. duckdb-wasm, already shipped by
the product, is 34 MB, so the planner adds roughly 4% to an existing
payload.

## Gotchas worth keeping

- `target/` is wiped by `mvn clean`, and the harnesses read
  `target/wasm/wasm-gc-module-runtime.js` — the POM unpacks it from
  `teavm-core` on every `package`, so always run `package` before a
  harness.
- The WASM build sees whatever core jar is **installed**, not what is
  in `core/src`. Re-install core before re-measuring, or the spike will
  cheerfully prove something about last week's code.
- `ServiceLoader` returns empty in WASM. Built-in section grammars are
  hardcoded and unaffected, but grammar *extensions* can never load in
  a WASM build.
