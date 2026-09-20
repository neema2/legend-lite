# legend-lite's planner, compiled to WebAssembly

A research spike, **not shipped code** and deliberately not a module of
the root reactor. It answers one question with measurements instead of
argument: *can the planner run in a browser, and does it still give the
same answers?*

**It can, and it does.** 69/69 queries byte-identical to the JVM,
refusals included.

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

- `target/` is wiped by `mvn clean`, and the harnesses read
  `target/wasm/wasm-gc-module-runtime.js` — the POM unpacks it from
  `teavm-core` on every `package`, so always run `package` before a
  harness.
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
