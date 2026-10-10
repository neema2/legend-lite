# Evidence: GraalVM Web Image against TeaVM for the tab (2026-10-10)

The experiment behind `docs/WEB_IMAGE_SPIKE_2026_10_10.md`: the tab's planner built a second way, by GraalVM Web Image
(Native Image's WebAssembly backend), and held to the same differentials as the TeaVM build. Everything ran by hand in
the worktree's scratch (`runs/webimage/`), outside Bazel; nothing in the build changed. Paths below are written
`<checkout>` (the worktree), `<output_base>` (`bazel info output_base`) and `<scratch>` (`<checkout>/runs/webimage`).

## Inputs

| What | Version | Where from | SHA-256 of the download |
|---|---|---|---|
| Oracle GraalVM (Web Image needs Oracle GraalVM 25.1 or later; Community Edition has no `svm-wasm`) | 25.4.4.1.1 (`native-image 25.0.4.1.1`, JVMCI 25.4-b23) | `https://gds.oracle.com/download/graal/25i4/archive/graalvm-jdk-25i4-25.0.4.1.1_macos-aarch64_bin.tar.gz` | `5980097f9c824b17872f9b59ee7dd1be6cd79f4626664f3fbecdcd5d8aa1705c` |
| Binaryen (Web Image assembles with its `wasm-as`; the build fails without it) | version_133, arm64-macos | `https://github.com/WebAssembly/binaryen/releases/download/version_133/binaryen-version_133-arm64-macos.tar.gz` | `ad66da82ac13f163e424b1643f16c6dfcccc98b5966296b43e52d3cab04f84a8` (matches the release's `.sha256`) |
| TeaVM | 0.15.0 (the repo's pin, `@maven_teavm`) | | |
| The planner's jars | main at 3304a48d9 | `bazel cquery //wasm:tab` (its JavaInfo's transitive runtime jars), copied out of the execution root | |
| Node | 22.16.0 (the repo's pin) | `<output_base>/external/rules_nodejs++node+nodejs_darwin_arm64` | |
| Chromium | Chrome for Testing 153 headless shell, revision 1243 (the repo's pin) | `bazel build //tools/browser:chromium_headless_shell` | |
| JVM for the probes | Zulu 25.0.4 (the repo's `remotejdk25`) and Oracle GraalVM 25.4's own `java` | | |

Machine: Apple Silicon, macOS 15, shared with other sessions (load 3 to 5 during the timings). Quote ratios, not
milliseconds.

## The files

| File | What it is |
|---|---|
| `adapter/WebImageExports.java` | The tab's adapter for Web Image: one functional object on `globalThis` (Web Image has no `@JS.Export` yet), dispatching to the SAME `TabExports` methods TeaVM exports, so both compilers compile the same Java |
| `harness/webimage-loader.mjs`, `teavm-loader.mjs` | One `load(dir)` for each module, the same exports shape, so one harness runs and times both |
| `harness/differential-webimage.mjs`, `round-trip-webimage.mjs`, `zone-webimage.mjs` | `wasm/differential.mjs`, `wasm/round_trip.mjs` and `wasm/zoneprobe.mjs`, unchanged in what they compare, the module swapped (`LOADER=./teavm-loader.mjs` for TeaVM), with timings |
| `harness/page.html`, `browser-bench.mjs` | The same three checks in the pinned Chromium: a local static server, the DevTools protocol, a fresh browser per run |
| `harness/teavm-main.mjs` | Runs a TeaVM module's `main(args)` (for the probes) |
| `probes/ClasslibProbe.java` | The guarantee: one million random doubles through `Double.toString`, `parseDouble` (shortest and long decimals), `BigDecimal.doubleValue`, `Float.toString`, plus `isBlank`/`strip`/`isWhitespace` over every `char`, each family folded to one SHA-256 |
| `probes/PortableProbe.java` | The same families with nothing TeaVM lacks (splitmix64 inputs, FNV-1a 64), so TeaVM runs it as the control |
| `analysis/sections.mjs` | A module's size by section |
| `analysis/attribute.mjs`, `wire.mjs`, `lambdas.mjs` | Each function's binary size, named from the `.wat`'s order, grouped (startup heap builders, lite's methods, the JDK's, ...); `wire.mjs` Brotli-compresses each group alone |
| `analysis/report.mjs` | Prints a Native Image build report's tables (`--emit build-report`) |
| `overlay/overlay.patch`, `SpikeFormat.java`, `OverlayCheck.java` | The measurement of dropping lite's two `ServiceLoader` uses and its `String.format` calls (the patch applies to main at 3304a48d9 and 0cbed2b9a); `OverlayCheck` proves the replacements spell exactly what `String.format` did |
| `teavm-full.patch` | `tools/teavm/TeaVmCompile.java` at `TeaVMOptimizationLevel.FULL` instead of `ADVANCED` (a scratch copy; the repo's tool is unchanged) |

## How it was run

```bash
G=<scratch>/dl-graal/graalvm-25.4.4.1.1+1.1/Contents/Home; B=<scratch>/dl-binaryen/binaryen-version_133/bin
NODE=<output_base>/external/rules_nodejs++node+nodejs_darwin_arm64/bin/nodejs/bin/node
# the adapter, against the planner's jars (cp/) and Web Image's API module
$G/bin/javac --add-modules org.graalvm.webimage.api -cp "cp/*" -d out/classes adapter/WebImageExports.java
# the build: res/ carries native/'s reachability-metadata.json (the prelude and the engine handlers as resources)
PATH=$B:$PATH $G/bin/native-image --tool:svm-wasm -H:-AutoRunVM --link-at-build-time \
    -cp out/classes:res:<cp jars> -o planner planner.WebImageExports
# the checks (the JVM's answers are Bazel outputs: //wasm:jvm_answers, //wasm:zone_jvm, //parser-equivalence:tab_round_trip)
$NODE --experimental-wasm-exnref harness/differential-webimage.mjs <build dir> wasm/corpus/model.pure wasm/corpus/queries.tsv bazel-bin/wasm/jvm_answers.txt
$NODE --experimental-wasm-exnref harness/round-trip-webimage.mjs <build dir> bazel-bin/parser-equivalence/generated/tab-round-trip.jsonl
$NODE harness/browser-bench.mjs <chrome-headless-shell> <web image dir> <teavm dir> <data dir> webimage|teavm [roundtrip]
# Binaryen after the build (the feature list is explicit: --all-features turns strings into stringref, which V8 refuses)
$B/wasm-opt --mvp-features --enable-gc --enable-reference-types --enable-exception-handling --enable-bulk-memory \
    --enable-sign-ext --enable-nontrapping-float-to-int --enable-mutable-globals --enable-multivalue -O1 in.wasm -o out.wasm
# why a type is in the image: the build stops and writes the call path to reports/
... native-image ... -H:AbortOnTypeReachable=java.util.Formatter ...
```

## Results

### Correctness: the repo's differentials against the Web Image build

| Check | Node 22.16 | Chromium 153 |
|---|---|---|
| Planner differential (`wasm/corpus`) | 69/69 (56 planned, 13 refused identically) | 69/69 |
| Timezone probe | 8/8 | 8/8 |
| Round trip in the tab | 9,423/9,423 (engine collection 9,134, of which 6,759 convert; showcase 123/123; lite's projects 166/166) | 9,423/9,423 |

### The guarantee: the class library is the JDK's

`ClasslibProbe 1000000 42`: all seven SHA-256 digests identical in Web Image (Node), on Oracle GraalVM 25.4's JVM and on
Zulu 25.0.4. `PortableProbe 1000000 42` (FNV-1a 64), the control:

| Family | JVM | Web Image | TeaVM 0.15 |
|---|---|---|---|
| `Double.toString(random bits)` | `8f7c091e31691049` | same | `5185705a0352fb7e` |
| `parseDouble(shortest text)` | `1ea7f3a8413c3613` | same | `89c50a07bc396761` |
| `parseDouble(long decimal)` | `40994b4fd4999a98` | same | `e0dd66cece7d0868` |
| `BigDecimal(long decimal).doubleValue` | `1a919e368a41e4e1` | same | same |
| `Float.toString(random bits)` | `346cb1b55f580bc4` | same | `298369e10c544550` |
| `isBlank/strip/isWhitespace`, 0 to FFFF | `6aa4ca635fc70408` | same | `dcc60a6c37026826` |
| `toString(parse(short decimal))` | `5d93fe21fe94cd39` | same | same |

### Speed (one harness for both; representative runs)

| | TeaVM `ADVANCED` | TeaVM `FULL` | Web Image |
|---|---|---|---|
| Node: load | 77 to 85 ms | 119 ms | 202 to 210 ms |
| Node: first plan | 2.86 to 3.19 s | 2.34 to 2.42 s | 255 to 258 ms |
| Node: 69 plans, warm | 2.47 to 2.52 s | 2.16 to 2.19 s | 224 to 230 ms |
| Chromium: load | 35 to 71 ms | 48 to 71 ms | 179 to 275 ms |
| Chromium: first plan | 2.05 to 2.79 s | 2.01 to 2.06 s | 220 to 243 ms |
| Chromium: 69 plans, warm | 1.85 to 2.72 s | 1.49 to 1.50 s | 185 to 215 ms |
| Round trip, 9,423 inputs | 25.0 s (Node), 22.1 s (Chromium) | 23.8 s (Node) | 21.4 s (Node), 22.6 s (Chromium) |

### Size

| Build | Raw | gzip -9 | Brotli 11 |
|---|---|---|---|
| TeaVM 0.15 `ADVANCED` (the repo's) | 5.03 MB | 1.83 MB | 1.32 MB |
| TeaVM 0.15 `FULL` | 14.88 MB | | 2.26 MB |
| Web Image, defaults | 19.50 MB | 7.06 MB | 4.97 MB |
| Web Image `-Os` / `-O1` / `-H:-AOTPriorityInline` | 19.48 / 19.47 / 19.51 MB | 6.95 / 6.93 / 6.95 MB | |
| + `wasm-opt -O1` (passes everything, Node and Chromium) | 18.14 MB | 6.13 MB | 4.40 MB |
| + `wasm-opt -Oz` (Chromium 69/69; Node 22: "type incompatibility when transforming from/to JS", every call) | 17.07 MB | 5.91 MB | 4.42 MB |
| + `wasm-opt -Oz --skip-pass=inlining --skip-pass=inlining-optimizing` (passes everything) | 17.34 MB | 6.13 MB | 4.45 MB |
| Web Image with the overlay (no `ServiceLoader` or `String.format` in lite) | 19.40 MB | 6.95 MB | |

Compression of the same module (MB): Brotli 11 with a 4 MB window 4.968, 16 MB window 4.966; zstd 19 (2^25 window)
5.425, zstd 22 5.424; gzip 9 7.059. After `wasm-opt -O1`: 4.401, 4.419, 4.840, 4.839, 6.130. TeaVM: 1.320, 1.319,
1.418, 1.418, 1.826.

### Where Web Image's bytes go

By section: code 14.09 MB (52,369 functions; TeaVM 3.36 MB, 23,815), data 2.80 MB (TeaVM 1.10), element 1.66 MB,
export 0.37 MB, type 0.27 MB, global 0.21 MB. The build: 8,188 types, 12,257 fields and 36,156 methods reachable (of
10,027, 19,816 and 60,230 loaded); an image heap of 9.76 MiB in 151,911 objects (49,515 `String`s with 2.45 MiB of
characters, 8,188 `Class` and 8,188 `DynamicHubCompanion`, 2,975 `ServicesCatalog$ServiceProvider`, 1,455
`ModuleDescriptor$Exports`, ...).

Each piece compressed alone (`wire.mjs`; the pieces sum to 4.91 MB against the whole file's 4.97):

| Piece | Raw | Brotli | Share |
|---|---|---|---|
| Startup heap builders (`fillHeapObjects*`: Web Image builds its image heap with instructions) | 5.94 MB | 1.63 MB | 33.1% |
| Lite's methods (static initialisers included) | 5.33 MB | 1.26 MB | 25.6% |
| The JDK's and the runtime's methods | 2.28 MB | 0.64 MB | 13.0% |
| Data: strings, names, tables | 1.42 MB | 0.46 MB | 9.4% |
| Element section (function tables) | 1.66 MB | 0.41 MB | 8.4% |
| Data: Pure source held in Java strings (`SystemMetamodel` and the like) | 1.08 MB | 0.18 MB | 3.6% |
| Generated helpers (allocators, field accessors) | 0.48 MB | 0.13 MB | 2.7% |
| Export, type, function, global sections | 0.96 MB | 0.16 MB | 3.3% |
| `prelude.pure` (300,247 bytes; 42 KB compressed alone) | 0.30 MB | 0.04 MB | 0.9% |

Reachable bytecode by package (the build report): lite's packages about two thirds (`resolver` 12.7%, `compiler`
11.1%, `protocol` 10.4%, `lowering` 7.3%, `parser` 7.0%, `sql` 5.2%, ...); Web Image's runtime 2.6%
(`com.oracle.svm`) and 2.1% (`org.graalvm.shadowed`: Guava and Jimfs, its in-memory file system); the JDK's largest
`java.util.concurrent` 1.5%, `java.util.stream` 1.4%, `java.util.regex` 1.4%, `java.time.format` 1.4%,
`jdk.nio.zipfs` 1.1%.

Lambdas: of lite's 3,737 types the build instantiates, 2,563 are lambda classes; the functions that exist only for
them are 9,357, 0.31 MB raw and 0.06 MB compressed. The rest of a type's cost is its share of the startup heap (its
`Class`, its companion, its name): estimated, not measured, at 0.15 to 0.2 MB for the lambdas.

Why a JDK subsystem is in the image (`-H:AbortOnTypeReachable`, first path of each trace):
- `jdk.nio.zipfs.ZipFileSystem`, `java.util.logging.LogManager`: Web Image's own file system (its shadowed Jimfs),
  from `FileSystems.getDefault`; not lite's.
- `java.util.Formatter`: `SystemMetamodel.<clinit>` (`String.formatted`). With the overlay: still reachable, through
  `java.net.URI`'s error messages (`jdk.internal.util.Exceptions.formatMsg`), from
  `ConnectionSectionGrammar.parseElasticsearchBody`.
- `java.util.ServiceLoader`: `SectionGrammarRegistry.build`. With the overlay: still reachable, through
  `ResourceBundle.getServiceLoader`.
- A trivial program (`PortableProbe`: 1,237 types, 4,157 methods) reaches none of `ServiceLoader`, `Formatter` or the
  services catalog.

The overlay: `OverlayCheck` found `SystemMetamodel`'s source text (106,516 characters) identical, and `pad`/`hex4`
equal to `String.format` on every value tried (widths 2, 3, 9 and 10 over -100,000 to 2,000,000; `%04x` over 0 to
FFFF). Reachable types 8,188 to 8,187; the module 0.5% smaller.

### Reproducibility

| The same input built twice | Result |
|---|---|
| Web Image, the planner | 19,503,297 and 19,437,759 bytes |
| Web Image, the planner, `--parallelism=1` | 19,429,834 and 19,454,773 bytes |
| Web Image, `PortableProbe` compiled with `-XDstringConcat=inline` (no `invokedynamic` in it) | 1,413,907 and 1,410,935 bytes; 4,769 and 4,768 functions |
| TeaVM 0.15, the planner (the repo's `//tools/teavm:compile`, the same class path twice) | identical (`59e7d5ea53890008...`) |

What differs between two Web Image builds: the order of the functions from the first one on; which functions exist
(167 differences in the planner after the hidden classes' addresses are set aside); which overload gets which name;
and the names of the hidden classes `switch` on types and string `+` make (`$$TypeSwitch_0x00000fff01d95260`), which
carry a memory address. A version-to-version delta is poor for the same reason: with the overlay (nine classes
changed), 18,481,958 of 19,401,554 bytes differ at the same offset, and zstd with a long window given the previous
version still costs 4.18 MB of the 5.43.

### Other facts

- Without `wasm-as` on the path the build stops ("'wasm-as' not found on the system path"); `-H:WasmAsPath=<path>`
  names it instead, so a Bazel rule can pass a pinned tool.
- `-H:-AutoRunVM` (main runs when the page asks, not on load) warns that it is experimental.
- Web Image's build: 35 s and 3.1 to 3.4 GB peak RSS (TeaVM at `FULL`: 53 s, in its 2 GB heap).
- TeaVM 0.16.0 (2026-10-02) leaves `DoubleAnalyzer`, `DoubleSynthesizer` and `FloatAnalyzer` as in 0.15, and its
  `String.isBlank` still tests only `' '`; TeaVM's issue #735 (open since 2023) is `parseDouble(toString(d)) != d`.
- Lite's browser code calls 488 distinct JDK methods (by class and name), most in `String` (34), `List` (25), `Map`
  (22), `BigDecimal` (22), `Stream` (21), `Set` and `Optional` (16 each).
