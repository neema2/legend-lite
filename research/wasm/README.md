# legend-lite's planner, compiled to WebAssembly

Started as a research spike — *can the planner run in a browser, and
does it still give the same answers?* **It can, and it does:** 69/69
queries byte-identical to the JVM, refusals included.

It is no longer only a spike. `datacube/src/wasm-planner.ts` is
product code built on the module this directory produces, and the
DataCube demo renders from it with no server running (see **Wired
into DataCube** below). What stays true is that this is not a module
of the root reactor and nothing in `core/` knows it exists: it
depends on the *installed* core jar, and the guardrails scan
`src/main/java` relative to their own module, so it is invisible to
them.

## Run it

```bash
# from the repo root. -am matters: it installs the PARENT pom as well
# as core, and without it the spike cannot read core's descriptor.
mvn -pl core -am -DskipTests install

cd research/wasm
mvn package                                      # → target/wasm/classes.wasm
node --experimental-wasm-exnref differential.mjs # 69-query differential
```

Verified on 2026-09-20 from a **fresh clone with an empty local Maven
repository** — every dependency, TeaVM included, resolves from Maven
Central. Note the absence of `-o`: the spike pulls artifacts a normal
`legend-lite` build never fetches, so the first run needs network even
on a machine whose `~/.m2` is otherwise warm.

`--experimental-wasm-exnref` is **required on Node 22**. TeaVM emits
the newer exception-handling opcodes, and without the flag the module
does not even compile: `Invalid opcode 0x1f`. Browsers that have
shipped WASM-GC need no flag.

Override `JAVA_HOME` and `LEGEND_CORE_JAR` if yours are elsewhere; the
defaults assume this machine's layout.

### Untested assumptions

Only one configuration has actually been run: **macOS on Apple
Silicon, Temurin 21.0.11, Node 22.16**. These are reasoned, not
measured — treat them as leads if something breaks:

- **Windows will not work.** The harnesses take
  `new URL(...).pathname`, which yields `/C:/...`, and join classpaths
  with `:`. Fixing it means `fileURLToPath` and `path.delimiter`.
- **Node older than 22 probably fails**, and not with a clear message:
  WASM-GC landed in V8 11.9, after Node 20's V8 11.3. Untried.
- **Linux is untried** but low-risk — the build is pure Java and the
  Node flag is not platform-specific.

The timezone probe is separate because it asks about a *resource*
rather than about code:

```bash
node --experimental-wasm-exnref zoneprobe.mjs > target/zone-wasm.txt
java -cp "${LEGEND_CORE_JAR}:target/classes" planner.ZoneMain > target/zone-jvm.txt
diff target/zone-jvm.txt target/zone-wasm.txt
```

The braces are load-bearing on **zsh**, which is the macOS default:
bare `"$LEGEND_CORE_JAR:target/classes"` parses `:t` as zsh's
tail-modifier, silently yielding
`legend-lite-core-1.0.0-SNAPSHOT.jararget/classes` and a
`ClassNotFoundException` that looks like a build problem.

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

## Wired into DataCube

`datacube/src/wasm-planner.ts` implements DataCube's `Planner`
interface on top of this module, so the cached-client plane needs no
server at all: snapshot -> Pure -> SQL -> DuckDB-WASM, in the tab.

```bash
cd ../../datacube
npm run planner:build     # mvn package in research/wasm
npm run planner:vendor    # copy classes.wasm + runtime js into demo/vendor
npm run build:demo        # builds BOTH bundles
npm run verify:wasm       # 16 cube shapes, DataCube's grammar, vs the JVM
npm run verify:browser    # the demo in headless Chromium, no server running
```

Two things about that wiring are deliberate.

**The planner is chosen at BUILD time, by entry point.** `main.ts`
wires the server planner, `main-wasm.ts` wires this one, and each
bundle can reach only its own. A `?planner=` URL switch was written
first and correctly rejected by `datacube/test/guardrails.test.ts`:
the rule there is that shipped code must not be able to CHOOSE a
planner at runtime, because then nobody can tell from the screen
which one produced the SQL and a divergence hides. A build-time
choice is static and visible, which is the same standard that test
already applies to stubs injected by tests.

**Two `implements Planner` files is not two planners.** The guardrail
bans a second IMPLEMENTATION — something that must agree with
legend-lite about null ordering, coercion and aggregates and will
eventually fail to. `WasmPlanner` is a second TRANSPORT to the same
`Compiler.plan`. That distinction is only worth anything because it
is checked, so the guardrail also requires the differentials to
exist: delete them and the second file must go too.

## What stops this from rotting

Nothing here runs in CI — building a WASM module on every push is not
worth the minutes. The property is guarded a cheaper way:
`PlannerSurvivesAotCompileTest` in the core suite bans the five APIs
that had to change, so a revert fails the normal build rather than
surfacing months later when somebody next runs `mvn package` in this
directory. That test is itself mutation-tested: each of the five
regressions was reintroduced in turn and confirmed to fail it.

It guards the API surface, not the outcome. Re-run the differential
by hand after any real change to the parser, typer, resolver or
lowerer.

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

- `target/` is wiped by `mvn clean`, and the harnesses import
  `target/wasm-runtime/org/teavm/backend/wasm/wasm-gc-module-runtime.js`
  — the POM unpacks it from `teavm-core` on every `package`, so always
  run `package` before a harness.
- TeaVM's loader wants a URL in a browser and a FILESYSTEM PATH under
  Node. Handing Node a `file:` URL fails with an ENOENT that surfaces
  as "could not instantiate" and reads like a missing WASM-GC feature;
  `WasmPlanner` converts, and says so.
- The WASM build sees whatever core jar is **installed**, not what is
  in `core/src`. Re-install core before re-measuring, or the spike will
  cheerfully prove something about last week's code.
- `mvn -pl core install` **without `-am`** appears to work and then
  fails here with `Could not find artifact com.legend:legend-lite:pom`
  — the parent pom never got installed. It only looks fine on a
  machine where some earlier full build left one in `~/.m2`.
- `ServiceLoader` returns empty in WASM. Built-in section grammars are
  hardcoded and unaffected, but grammar *extensions* can never load in
  a WASM build.
