# Phase 1: generator hygiene, the concrete edit list (2026-10-06)

Worktree `runs/build-rebuild`, branch `build/phase1-generators` at `7950fafb2` (stacked on PR #25). The dossiers were
written at `669b39ad1`; no generator BUILD file, program or source they cite changed since (`git diff 669b39ad1 HEAD`
touches `.bazelrc`, comments in `MODULE.bazel` and `release.MODULE.bazel` (pins unchanged), `tools/deps`,
`tools/guards`, `tools/jars`, `tools/js`, three package.json/lock pairs, the CI cache key, docs and the root `sites`
comment), and every line below was re-read at `7950fafb2`. Line numbers are HEAD's: while this was written the
working tree already took §1's two deletions (uncommitted: `parser-equivalence/BUILD.bazel`, `spec/BUILD.bazel`,
`EagerCorpusCompileProbe.java`, `MigrationSizing.java` staged for deletion); that probe now has 8 harness sites, so
`HarnessDisciplineTest.java:133` must say 8 in the same commit or `//core:census` fails (§1.2).
Queries run (one shared server): `rdeps(//..., set(<testonly candidates>), 1)`, `rdeps(//..., set(<manual and dead
candidates>), 1)`, `attr(tags, "\bmanual\b", set(...))`.

Scope reminders: the measurement group stays as is (judges, ladder, ratchets, catalog_corpus, saved-queries, reference
lane, gen_roster, keyword_coverage's measurement role); `gen_claims`/`core_next`/native-claims.tsv are Phase 5 (D2);
gen_natives, gen_dynafn, gen_engine_handlers and gen_prelude are restructured in Phases 2 to 5 (their Phase 1 share is
stated per item in §5). Where a change touches a set-aside target as a side effect it says **SIDE EFFECT**.

---

## 1. Dead (delete)

### 1.1 `//parser-equivalence:migration_sizing`
- Delete the `_REPORTS` row `parser-equivalence/BUILD.bazel:421`. It leaves `:diagnostics_reports` (`:450-455`, a
  comprehension over `_REPORTS`) and `:guard_classpaths` by itself.
- Delete `parser-equivalence/src/test/java/com/legend/equivalence/MigrationSizing.java` (in `pe_tests_lib`'s glob,
  `:50`; referenced only by itself).
- No writer, diff test, suite, `//:generated`/`//:update_generated` entry, gate or CI target names it (query: its only
  rdeps are `:diagnostics_reports` and `:guard_classpaths`; `git grep` of `.github/`, `gates/`, root `BUILD.bazel`:
  none).
- Leave: `core/src/test/java/com/legend/SkipCensusTest.java:60` and `docs/PARSER_COMPLETENESS_PLAN.md:273` (they name
  the long-deleted `MigrationSizingTest`, history); `docs/build-inventory/*` (dated records).
- Why dead: both of its paths run the protocol parsers since `b23f68757` deleted the legacy ones; nothing reads its
  output.

### 1.2 `//spec:eager_corpus_compile_world2`
- `spec/BUILD.bazel:532`: delete the `("eager_corpus_compile_world2", ["-Deager.world2=1"])` tuple; `:514-516`: drop
  the world-2 sentence (optionally flatten the one-entry comprehension `:517-533` into a plain `java_run` named
  `eager_corpus_compile`, same attributes).
- The code only it reaches: `spec/src/test/java/com/legend/rcorpus/EagerCorpusCompileProbe.java:105-175`: keep
  `:106-108` unconditional (write `eager-corpus.txt`, print `out.get(0)`, `out.get(1)`), delete the
  `-Deager.world2` branch `:111-175`; javadoc `:15-20`: drop the world-2 sentence.
- Pins that move with that code (both `@Tag("census")`, so `//core:census`, the checks lane and `//gates:local`):
  - `core/src/test/java/com/legend/HarnessDisciplineTest.java:133` is EXACT-MATCH (`assertEquals`, `:248`):
    `Map.entry("EagerCorpusCompileProbe.java", 19)` becomes `8`. The 11 sites that go are probe `:120, :147, :153,
    :155, :157` (x3), `:158, :170, :171, :172` (counted with the test's own `SITE` regex). Dated comment per
    AGENTS.md.
  - `core/src/test/java/com/legend/architecture/ParserBoundaryArchTest.java:87-90`: remove the probe's
    `DIALECT_CLASSES` entry (shrink-only list; its only `Dialect.LEGEND_PLATFORM` use is probe `:133`, in the deleted
    branch). Not removing it passes too (no staleness check), but leaves a stale allowance.
- No writer, diff test, suite, root wiring, gate or CI target (query: only `//spec:guard_classpaths`).
- Smaller alternative: the BUILD tuple only; the `-Deager.world2` branch then stays as unreachable code and no pin moves.

Not dead in Phase 1: `//spec:gen_claims`, `//spec:claims_generator_lib`, `//core:core_next`, `:core_next_prelude`,
`//core:update_generated_4` (D2, Phase 5).

---

## 2. Manual

Lanes: none of these is named by a CI lane, a `:all` wildcard (`//tools/deps:all`, `//wasm:all`;
`.github/workflows/gates-run.yml:51,61`) or `//gates:local` (`gates/BUILD.bazel:11-80`). Each is built today only by
`bazel build //...` (the build lane, `gates-run.yml:63`) and by the bump's phase 3 `bazel test //...`
(`tools/bump/Bump.java:153-157`), which builds every non-manual target. Tagging changes only those two; a manual target
named on the command line (`bazel build`, `bazel run`) still builds.

| Target (BUILD:line) | Today | Change |
|---|---|---|
| `//spec:native_declarations` (`spec/BUILD.bazel:417-433`) | not manual; only rdep `//spec:guard_classpaths` (analysis-only) | `tags = ["manual"]`; comment `:415-416` adds "manual: asked for". The build lane stops reading ~2,265 upstream `.pure` and parsing the native-bearing ones |
| `//spec:native_membership_draft` (`spec/BUILD.bazel:437-445`) | not manual; rdep `//core:draft_native_membership` | `tags = ["manual"]` |
| `//core:draft_native_membership` (`core/BUILD.bazel:832-836`, `diff_test = False`) | not manual; no rdep | `tags = ["manual"]` (kwargs reach the writer, `bazel_lib write_source_files.bzl:189-203`) |
| `//datacube:link_dictionary_next` (`datacube/BUILD.bazel:309-314`) | not manual, on purpose: "built with //... so the tool cannot rot unseen" (`:305`); rdep `:cut_link_dictionary` | `tags = ["manual"]`; rewrite the comment `:305-308`. Lost: the build-time run (type rot stays caught by `//datacube:typecheck_test`, `:246`; a runtime throw in the writers is no longer caught; a small `makeDictionary()` test would be its replacement, not Phase 1). Its rules_js helper targets are already manual |
| `//datacube:cut_link_dictionary` (`datacube/BUILD.bazel:316-321`) | not manual | `tags = ["manual"]` |
| `//parser-equivalence:gen_own_corpus_draft` (`parser-equivalence/BUILD.bazel:392-413`) | not manual; rdep `//docs:draft_own_corpus_ledger` | `tags = ["manual"]`. **FLAG:** GENERATORS.md §5 lists it under the set-aside measurements ("on demand"); do it only with the user's OK |
| `//docs:draft_own_corpus_ledger` (`docs/BUILD.bazel:27-34`) | not manual | `tags = ["manual"]` (same flag) |

Already manual (no change): `//parser-equivalence:corpus_census`, `:grammar_keyword_census`,
`:pmcd_reachability_census` (all `:446`; the last is replaced in §6.5), `:diagnostics_reports` (`:454`),
`//spec:eager_corpus_compile` (`:528`), `//spec:reference_lane_report` (`:256`), `//tools/reference:ref_dump` (`:81`),
`//tools/reference:ref_imports` (`:107`).

Not in Phase 1 (GENERATORS §6 step 6, Phase 7), noted because it is the one manual target `//...` still builds:
`//:update_generated` (`BUILD.bazel:106-126`) is not manual and lists `//pct:update_ratchets`, so `bazel build //...`
runs the manual 2 GB `//pct:ratchets` (`pct/BUILD.bazel:256-272`). Tagging the root writer `manual` would change only
the build lane (and phase 3 of the bump); the bump runs it by name. See §7 (pull-forward option).

---

## 3. testonly

Consumers from `rdeps(//..., set(...), 1)` (guards excluded; `guard_classpaths`/`guard_markdown` are themselves
testonly, `tools/guards/defs.bzl:95,105`). `.bazelrc:110` turns on `--incompatible_check_testonly_for_output_files`, so
a non-testonly target that consumes an OUTPUT FILE of a testonly target fails analysis too.

| Target (BUILD:line) | Direct consumers today | Non-testonly consumer that must change |
|---|---|---|
| `//scripts/corpus:gen_differential` (`scripts/corpus/BUILD.bazel:188-204`) | its `:diff/seed.sql` and itself as data of `//core:corpus_differential_test` (`core/BUILD.bazel:465-479`, a test) | none |
| `//core:stress_index` (`core/BUILD.bazel:747-751`) | `//core:core_tests_lib` (testonly, `:301-344`) | none |
| `//datacube:offer_queries` (`datacube/BUILD.bazel:323-335`) | its two outputs, srcs of `//datacube:offer_facts` (`:345-359`) | **`//datacube:offer_facts` → `testonly = True`**; its output feeds `//datacube:update_generated_0` and `_0_test` → **the whole `//datacube:update_generated` call (`:421-431`) → `testonly = True`** (writers `_0.._3` and the umbrella; its only rdep, `//:update_generated`, is testonly, `BUILD.bazel:108`) |
| `//datacube:cube_queries` (`datacube/BUILD.bazel:446-461`) | `//datacube:cube_jvm_answers` | `cube_jvm_answers` (itself in this list) |
| `//datacube:cube_jvm_answers` (`datacube/BUILD.bazel:463-479`) | `//datacube:wasm_differential_test` (`:481-493`, a test) | none |
| `//wasm:jvm_answers` (`wasm/BUILD.bazel:96-111`) | `//wasm:differential_test` (`:113-126`) | none |
| `//wasm:zone_jvm` (`wasm/BUILD.bazel:130-136`) | `//wasm:zone_test` (`:138-147`) | none |
| `//tools/java_run:pins` (`tools/java_run/BUILD.bazel:17-23`) | `:pins_test` (`:25-29`, a `diff_test`) | none |
| `//tools/java_run:print_pins` (`tools/java_run/BUILD.bazel:10-15`) | `:pins` | none (made testonly with it) |

Already testonly: `//tools/reference:ref_dump` (`:66`), `//pct:adapter_par` (`pct/BUILD.bazel:38`). Product outputs,
never testonly: `//warehouse:duckdb_library`, `//warehouse:duckdb_extensions`, `//datacube:dist`. `stress_index` and
`stress_layout` are Starlark rules; `testonly` is a common attribute (`core/stress.bzl:45-64` need no change);
`stress_layout` stays non-testonly (its output feeds the non-testonly `gen_dense`/`gen_stress`).

Check: `bazel build --nobuild --config=bazel10 //...` (testonly is enforced at analysis).

---

## 4. PCT adapter: constant jar timestamps

**Today.** `//pct:adapter_par` (`pct/BUILD.bazel:36-53`) is a `java_run` (`tools/java_run/defs.bzl:44-120`) of
`com.legend.tools.par.ParGenerator` (`tools/par/ParGenerator.java:29-52`), which calls legend-pure's
`PureJarGenerator.doGeneratePAR` (`:39-48`). That writes the PAR through `PureRepositoryJarBuilder`
(pinned legend-pure, `legend-pure-m3-core/.../serialization/runtime/binary/PureRepositoryJarBuilder.java`): `:44` `new
JarOutputStream(stream, PureManifest.create(...))` writes the manifest entry, and `:59, :102, :129`
`putNextEntry(new JarEntry(name))` never set a time, so the JDK's `ZipOutputStream.putNextEntry` stamps each entry with
`System.currentTimeMillis()`. Evidence, today's `bazel-bin/pct/pure-core_legend_lite_pct.par`: 6 entries
(`META-INF/MANIFEST.MF`, `META-INF/definition-index.json`, `META-INF/reference-index.json`,
`core_legend_lite_pct/pct_{adapter,native,types}.pc`), all DEFLATED, all dated `2026-10-06 14:10:02` (the build time),
the first carrying the JAR magic extra field `0xCAFE`. G3 §10.9 found identical CRCs across three builds with different
file hashes. legend-pure's PAR reader never reads entry times (no `getTime`/`lastModified` under its
`serialization/runtime/binary/`). Effect: every cache miss gives new bytes, so all 11 Channel A tests, the 5 Channel B
tests and `//pct:ratchets` rerun.

**Fix (in our program; no Bazel built-in fits).** singlejar's `--normalize` rewrites the manifest (the PAR's
`PureManifest` attributes would be lost) and `@bazel_tools//tools/zip:zipper` cannot rewrite in place without a shell
step. So, in `tools/par/ParGenerator.java`, after the `isFile` check (`:49-51`), rewrite the PAR in place:

```java
// legend-pure stamps every entry with the clock (PureRepositoryJarBuilder: new JarEntry, no time), so two builds of
// the same PAR differed only there. Entries, order and bytes are kept; only the time is one constant.
private static final java.time.LocalDateTime FIXED = java.time.LocalDateTime.of(2010, 1, 1, 0, 0); // Bazel's jar epoch

private static void normalize(java.nio.file.Path par) throws IOException {
    record Entry(String name, byte[] bytes) {}
    List<Entry> entries = new ArrayList<>();
    try (java.util.zip.ZipFile zip = new java.util.zip.ZipFile(par.toFile())) {   // central-directory order = write order
        for (var e = zip.entries(); e.hasMoreElements(); ) {
            var ze = e.nextElement();
            try (InputStream in = zip.getInputStream(ze)) { entries.add(new Entry(ze.getName(), in.readAllBytes())); }
        }
    }
    try (var out = new java.util.jar.JarOutputStream(java.nio.file.Files.newOutputStream(par))) { // re-adds 0xCAFE to entry 1
        for (Entry e : entries) {
            var je = new java.util.jar.JarEntry(e.name());
            je.setTimeLocal(FIXED);               // DOS time, no timezone conversion, no extended-timestamp field
            out.putNextEntry(je);                  // DEFLATED, as the original
            out.write(e.bytes());
            out.closeEntry();
        }
    }
}
```

Manifest stays first (the JarInputStream contract). No BUILD change. Optional permanent check (one per platform in CI
with no workflow edit): a small junit test that opens `$(rlocationpath //pct:adapter_par)` and asserts every entry's
`getTimeLocal()` equals `FIXED`, added to `//pct:pct_duckdb`'s `tests` (`pct/BUILD.bazel:157-160`, lane 6).

Check: build, copy the PAR, delete it from `bazel-bin`, rebuild with `--disk_cache=`, `cmp` (this also settles G3's
OPEN on content variance from legend-pure's identity-hash collections); lanes 6, 7, 7p, 9 rerun once.

---

## 5. Narrowing, per generator

Visibility: every `//core` library is private (`core/BUILD.bazel:19`); a narrower dep needs one visibility line in
`core/BUILD.bazel` each (the sanctioned path, `:59-60`). The full set this section adds:
`:diagnostics` (`:70`) → `//parser-equivalence`; `:protocol` (`:73`), `:parser` (`:75-79`), `:sql_dialect`
(`:101-105`), `:database` (`:110-114`), `:compiler` (`:117-121`), `:plan` (`:142-146`), `:planner` (`:178-186`) →
`//datacube`; `:compiler_element_type` (`:96-100`) → `//datacube`, `//engine-client`; `:lowering` (`:137-141`) →
`//wasm`. Layering (`tools/deps/core-layers.txt`) and `//tools/deps:core_closure_test` are unaffected (no dep of a core
library changes).

New shared constants, `tools/generators/defs.bzl` (after `:22`):
```starlark
# legend-engine's tree alone, for a generator that reads no legend-pure source
ENGINE_TREE = ["@legend_engine_src//:pom.xml", "@legend_engine_src//:tree"]
ENGINE_ROOT = {"@legend_engine_src//:pom.xml": "{ENGINE_ROOT}"}
```

### 5.1 Section 2 (upstream)

**gen_fixtures** (`parser-equivalence/BUILD.bazel:280-297`). Now: srcs `UPSTREAM_TREES` + tests-jars + pins; roots
`UPSTREAM_ROOTS`; deps `:harvest_lib` (`:247-258`) = `:harvest_shims` (deps `:pe_tests_lib`, `:230`) + `:pe_tests_lib`
+ tests-jars + json-unit, so all 31 core jars, `//json`, every PE test class and `src/test/resources/**` (its own
committed output among them). Reads (verified): the 2 tests-jars by `-Dlegend.harvest.jars`
(`FixtureHarvestGenerator.java:42-47`); three engine modules' `src/test/java` by `-Dlegend.engine.root` (`:56`);
`OraclePins.engineRelease()` and the constant `Corpus.FIXTURE_HEADER_PREFIX` (`FixtureRecorder.java:39-40`,
`Corpus.java:157`); `//testing`'s `ProgramPaths`; the class path's engine jars (ServiceLoader). No pure tree
(`program_jvm_flags(..., pure = False)`, `:290`), no core class (imports of the 6 harvest files: `java.*`, engine,
`FixtureRecorder` only). Change:
```starlark
# the five classes the corpus programs share (closure checked: Corpus -> OraclePins, InlineSnippets; InlineSnippets ->
# Corpus, ModuleFiles; ModuleFiles -> Corpus; ManifestGenerator -> Corpus)
_CORPUS = ["src/test/java/com/legend/equivalence/%s.java" % c for c in
           ["Corpus", "InlineSnippets", "ManifestGenerator", "ModuleFiles", "OraclePins"]]
_HARVEST_PROGRAM = ["src/test/java/com/legend/equivalence/harvest/%s.java" % c for c in
                    ["FixtureHarvest", "FixtureHarvestGenerator"]]
# the engine's side of every parity program's class path: ONE list, so the oracle, the harvest and the reachability
# record see the same engine (the 11 @maven_upstream deps now at :56-66, and the 8 extension grammars at :71-78)
_ENGINE_COMPILE = [...]
_ENGINE_EXTENSIONS = [...]
_JUNIT5 = ["@maven_test//:org_junit_jupiter_junit_jupiter_api", "@maven_test//:org_junit_jupiter_junit_jupiter_params"]

legend_java_library(name = "pe_corpus", nullaway = False, testonly = True, srcs = _CORPUS,
    deps = ["//core:diagnostics", "//testing", "@maven_upstream//:com_fasterxml_jackson_core_jackson_databind"])
legend_java_library(name = "harvest_program", nullaway = False, testonly = True, srcs = _HARVEST_PROGRAM,
    deps = ["//testing"])
```
- `pe_tests_lib` (`:44-80`): `srcs = glob([...], exclude = _HARVEST_SHIMS + _HARVEST_PROGRAM + _CORPUS)`;
  `deps = ["//core", "//testing", ":pe_corpus"] + _ENGINE_COMPILE + _JUNIT5`; `runtime_deps = _ENGINE_EXTENSIONS`.
  (Package-private `Corpus`/`ModuleFiles` members stay reachable: same package, one class loader.)
- `harvest_shims` (`:222-241`): `:pe_tests_lib` (`:230`) → `:pe_corpus`.
- `harvest_lib` (`:247-258`): `runtime_deps = [":harvest_shims", ":harvest_program"] + _ENGINE_COMPILE +
  _ENGINE_EXTENSIONS + _JUNIT5 + [grammar_tests, compiler_tests, json_unit]` (shims first, then the engine, then the
  tests-jars grammar before compiler, as now: today's external jar set minus our jars).
- `gen_fixtures`: `srcs = ENGINE_TREE + [":harvest_tests_jar_files", ":harvest_tests_jars", "//tools:oracle-pins.env"]`,
  `roots = ENGINE_ROOT`.
- core: `:diagnostics` visibility (only because `pe_corpus` carries `Corpus.load`'s `Diagnostics.value`,
  `Corpus.java:275`, inert in the generators).
- Result: no `//core` but `:diagnostics`+`//base` (2 jars, never loaded by the harvest path), no PE test class, no PE
  resource. Refinement (drops those 2 jars): move `FIXTURE_HEADER_PREFIX` beside `OraclePins` into a `:pe_pins`
  library and point `FixtureRecorder.java:39` at it (2 Java edits).
- Self-input: gone (the committed `engine-grammar-fixtures.jsonl` was in `pe_tests_lib`'s resources).
- **Risk:** the harvest's class path changes. The external jars are kept identical, so ServiceLoader finds the same
  extensions and tier 2 compiles against the same engine jars; `//parser-equivalence:update_generated_1_test`
  compares the result with the committed snapshot, and the `@@ tier 1/2` receipt lines can be compared before/after.
  **SIDE EFFECT:** `pe_tests_lib` (gate 8; gen_roster, PE ratchets, censuses) loses 7 sources to two new libraries.

**gen_manifest** (`:304-318`). Now: deps `:pe_tests_lib` (all core, every PE class and resource incl. its own output).
Reads: both trees, the fixtures OUTPUT (`-Dlegend.engine.fixtures`, `Corpus.java:179-188`), the pins; classes
`ManifestGenerator`, `Corpus`, `InlineSnippets`, `ModuleFiles`, `OraclePins`, `//testing`, `Diagnostics` (inert),
Jackson. Change: `deps = [":pe_corpus"]` (srcs, flags unchanged; it does read both trees). Self-input gone.
Risk: none (nothing parses; same classes). Plus, for the set-aside generators that keep `pe_tests_lib`:
`pe_tests_lib` `resources = glob(["src/test/resources/**"], exclude = ["src/test/resources/corpus-manifest.tsv",
"src/test/resources/engine-grammar-fixtures.jsonl"])` (`:51`): no class reads either from the class path (tests read
the snapshot by runfiles path, `Corpus.java:178-188`; the manifest is read by nobody), so a bump's rewrite of them stops
rerunning every PE program. Risk: none.

**vocab** (`tools/engine-runner/BUILD.bazel:60-69`). Now: deps `:runner` (`:9-45`: `//core` + 8 `@maven_runner`
deps + 16 runtime_deps), built twice (target, and `opt-exec` through `keyword_coverage`'s tool data). Reads: every jar
on its class path (`TokenDump.java:41-66`); imports only `org.antlr.v4.runtime.Vocabulary` (`:3`); our jars add no line
(no ANTLR `VOCABULARY`). Change:
```starlark
_RUNNER_DEPS = [...]       # :36-43, as today
_RUNNER_RUNTIME = [...]    # :17-32, as today
legend_java_library(name = "runner", ..., srcs = glob(["src/main/java/**/*.java"],
    exclude = ["src/main/java/perf/TokenDump.java"]), runtime_deps = _RUNNER_RUNTIME, deps = ["//core"] + _RUNNER_DEPS)
# TokenDump alone, over the runner's jars without core (it scans every jar on its class path; ours carry no lexer)
legend_java_library(name = "token_dump", nullaway = False, testonly = True,
    srcs = ["src/main/java/perf/TokenDump.java"], runtime_deps = _RUNNER_RUNTIME, deps = _RUNNER_DEPS)
```
`vocab`: `deps = [":token_dump"]`; drop `visibility` (`:67`, nothing outside names it after 5.3). `TokenDump.java:28`
"Consumed by keywords.py" → "read by scripts/parser/fixtures.py (no target); kept as the bump's record".
Risk: low; external jars and their order kept (minus ours); `//tools/engine-runner:update_vocab_test` holds the bytes.

**gen_imports** (`spec/BUILD.bazel:331-347`). Now: deps `:generators` → `//core` + `:claims` + `:source_tree`.
Reads: two files' text only (`ImportsGenerator.java:40-41`; imports `java.*` only, `:6-13`). Change (can land before
§6.6, output unchanged):
```starlark
legend_java_library(name = "imports_generator", nullaway = False,
    srcs = ["src/gen/java/com/legend/generators/ImportsGenerator.java"])
```
`:generators` glob (`:59-62`) also excludes `ImportsGenerator.java`; `spec_tests_lib` deps (`:99-112`) add
`:imports_generator` (`CoreImportsParityTest.java:55` calls `ImportsGenerator.metaImports`); `gen_imports`
`deps = [":imports_generator"]`. Self-input (its committed file compiled into the `//core` it ran on): gone.
Risk: none. **SIDE EFFECT:** `spec_tests_lib` gains a dep (spec ratchets and the judges rerun once, same bytes).

**gen_dynafn** (`spec/BUILD.bazel:350-366`): Phase 1 = drop the pure tree only. `srcs = ENGINE_TREE +
["//core:src/main/java/com/legend/builtin/DynaFn.java"]`, `roots = ENGINE_ROOT` (args `:356-360` use only
`{ENGINE_ROOT}`; no jvm_flags). Its library (`:generators` → `//core`) and its self-inputs (committed DynaFn.java
spliced and compiled, committed engine-handlers.tsv through `EngineHandlers.fqnsOf`) go in Phase 2 (DynaFn generated
whole, `DynaFnDecisions`). Risk: none (`//core:update_generated_0_test`).

**gen_natives** (`spec/BUILD.bazel:370-389`): no Phase 1 change. Over-declared (engine files outside the 3 spec roots,
non-`.pure` files, most core jars, `:claims`) and self-input (committed Pure.java spliced and compiled) all retire with
the generator in Phase 5.

**gen_engine_handlers** (`spec/BUILD.bazel:316-328`): no Phase 1 change. Its self-input (its own engine-handlers.tsv in
`//core:builtin`'s resources, `core/BUILD.bazel:89-92`) survives any library narrowing that keeps `:builtin`; Phase 2
emits the table with no core dependency.

**gen_prelude** (`spec/BUILD.bazel:450-469`): no Phase 1 change (Phase 4 replaces it).

**ref_imports** (`tools/reference/BUILD.bazel:98-109`): already narrow (deps `_REFERENCE_JARS` only). Committing it:
§6.4.

**pmcd_reachability_census**: split, §6.5.

**native_declarations**: manual only (§2); narrowing comes with Phase 5 (it becomes the catalog).

**gen_claims**: Phase 5.

### 5.2 Section 4 (ours)

**gen_dense** (`scripts/corpus/BUILD.bazel:103-114`). Now: srcs `_INPUTS` (`:90-94`, incl. `queries.pure`), tool
`:dense_build` on `:corpus` (29 modules + tzdata). Reads: 192 stress sources, 31 project files, `$STRESS_LAYOUT`, a
14-module closure (AST walk of every import, function-local included: aggregate combos dense_mapping dense_store
exactmath expand flat model oracle partition query rhs seed views); `query.load()` (the only `queries.pure` reader,
`query.py:398-399`) is called from `build.py:152`, `differential.py:161`, `executed.py:408`, `oracle.py:2729` only.
Change: `_SOURCES = ["//core:stress_layout", "//core:stress_sources"] + _LINKED`; `gen_dense` `srcs = _SOURCES`;
`py_library(name = "corpus_dense", srcs = [m + ".py" for m in _DENSE_MODULES], imports = ["."], deps =
["@pypi//tzdata"])`; `:dense_build` deps `[":corpus_dense"]`. Risk: low (a missed import fails the action loudly;
`//core:update_stress_corpus_{0,1,2}_test`).

**gen_stress** (`:118-131`). Reads the 27-module closure (all but dense_mapping, dense_store), `queries.pure`, the
dense OUTPUTS. Change: `srcs = _SOURCES + ["queries.pure", ":gen_dense"]`; `py_library(name = "corpus_build", 27
modules)`; `:build` deps `[":corpus_build"]`. `:corpus` (`:15-53`) stays for the five gates (`:150-177`). Risk: low
(`_3.._9_test`; 146 s when it runs).

**stress_layout**: exact (analysis-time write of `core/stress.bzl`). No change.

**catalog_rules** (`datacube/BUILD.bazel:378-384`). Now: `catalog_facts_main` (`:365-376`) deps `//core`, `//json`.
Reads in `rules` mode: `CatalogType`, `DuckDb`, `Postgres`, `CATALOG_COLUMNS_SQL` (`//core:sql_dialect`;
`CatalogFacts.java:65-130`). The library is shared with `catalog_corpus`, whose mode needs `SpecParser`,
`ProtocolEmitter`/`SourceInformation`, `Json`, `Databases` (`:6-14`, `:260`). Change (A, no Java edit):
`catalog_facts_main` deps `["//core:database", "//core:parser", "//core:protocol", "//core:sql_dialect", "//json"]`
(runtime `@duckdb_jdbc_warehouse//jar` kept): 12 libraries instead of 33. **SIDE EFFECT:** narrows the set-aside
`catalog_corpus` the same way (its output held by `//datacube:update_generated_2_test`). (B, GENERATORS' "3 libraries":
split the `rules` mode into its own class and library on `//core:sql_dialect`; edits `CatalogFacts.java`, and keep
`HEAD`'s text or catalog-facts.ts's header line moves.) Risk: low (compile errors are loud; `_1_test`).

**offer_facts** (`datacube/BUILD.bazel:345-359`). Now: `offer_facts_main` (`:337-343`) deps `//core`. Reads
(`OfferFacts.java:6-17`): `Compiler`, `TypedQuery` (`:planner`), `NameResolver`, `ModelContext`, `TypedFunction`,
`TypedParameter`, `TypedNativeCall`, `TypedSpec` (`:compiler`), `Type` (`:compiler_element_type`),
`UpstreamRelationType` (`:plan`), `ProtocolReader`, `AppliedFunction`, `LambdaFunction` (`:protocol`). Change: deps
`["//core:planner", "//core:compiler", "//core:compiler_element_type", "//core:plan", "//core:protocol"]` (the planner
closure, 24 jars). One-line alternative: `["//core:plan_side"]` (`core/BUILD.bazel:226-230`, +`//datacube` on its
visibility; 26 jars). Risk: low (`_0_test`). Self-input loop through `offer_queries` (its tool's `:src` includes the
committed `src/generated/offer-facts.ts`, imported by `calc.ts:30`) stays: it reruns one Node action with identical
bytes, so `offer_facts` is cut off. Narrowing `emit_offer_queries`' data to its import closure is not GENERATORS §4
work.

**test_imports** (`datacube/BUILD.bazel:409-419`): already narrow. GENERATORS §4's "not 122 npm files" does not apply
to it: its tool (`:395-402`) has no data and its srcs are `:import_scan` (`:404-407`); the 122 npm files belong to
`offer_queries`, `cube_queries` and `link_dictionary_next` (through `:src`). No change.

**lite_facts** (`engine-client/BUILD.bazel:47-53`). Now: `type_facts_main` (`:39-45`) deps `//core`. Reads `Type`,
`PlatformTypes` (`TypeFacts.java:6-7`). Change: deps `["//core:compiler_element_type"]` (13 jars). Risk: low
(`//engine-client:update_generated_test`).

**icons_gen** (`legend-art/BUILD.bazel:103-109`): exact. No change.

**reachability_metadata** (`warehouse/BUILD.bazel:388-396`). Now: deps `:tests_lib` (every warehouse test class,
client, testing, JUnit, the DuckDB jar) → `:server_lib`, whose resources (`:74-75`) include the committed output: a
self-input. Reads: `Duck`, `AuthenticatedUser` (same package, package-private; imports `java.*` only,
`ReachabilityMetadata.java:6-18`). Change:
```starlark
legend_java_library(name = "reachability_metadata_lib", nullaway = False, testonly = True,
    srcs = ["src/test/java/com/legend/warehouse/server/duck/ReachabilityMetadata.java"], deps = [":server_lib"])
# the native image's metadata, out of :server_lib, so its generator never reads what it writes
legend_java_library(name = "native_image_metadata",
    resources = ["src/main/resources/META-INF/native-image/com.legend/warehouse/reachability-metadata.json"],
    resource_strip_prefix = "warehouse/src/main/resources")
```
`tests_lib` srcs (`:106-113`) exclude `ReachabilityMetadata.java`; `reachability_metadata` deps
`[":reachability_metadata_lib"]`; `server_lib` `resources = glob(["src/main/resources/**"], exclude =
["src/main/resources/META-INF/native-image/**"])`; `server_native` (`:192-213`) `deps = [":server_lib",
":native_image_metadata"]` (native-image reads `META-INF/native-image/**` from any class-path jar; `native_image`'s class
path is its deps' runtime jars, rules_graalvm `internal/native_image/rules.bzl:43-46`). A resources-only library is
`java_library JavaResourceJar`, allowed in the native tier (`tools/guards/CompileOnlyTest.java:42,57`).
**Risk: medium:** the native image's inputs change (native lane; `bazel build //warehouse:server_native`); `//:java`'s
warehouse jar loses the JSON (the JVM server never read it).

### 5.3 Named in GENERATORS §6 steps 1-3 though listed in §5

**keyword_coverage** (`scripts/parser/BUILD.bazel:73-94`). Now: `tool = ":keywords"` (`:93`), whose `data = _DATA`
(`:24-31`) includes `//tools/engine-runner:vocab` (`:28`): its runfiles drag `:runner` and all of core into `opt-exec`
(40 exec Javac actions) and a second vocab. Reads with `--out`: `--engine-root` and `--ours` only
(`keywords.py:372-385`); `RUNNER_VOCAB` is optional (`:116`) and `runner_vocabulary`/`version_skew` are never called
from `main()`. Change (the named item): delete `"//tools/engine-runner:vocab",` (`:28`) and `"--vocab",
"$(rootpath //tools/engine-runner:vocab)",` (`:41-42`); comment `:1-3`. Optional (beyond the named item: the rest of
`_DATA` are source files, no exec rebuild): a data-less tool `py_binary(name = "keywords_tool", testonly = True, srcs =
["keywords.py"], main = "keywords.py", deps = [":tiers"])` as `keyword_coverage`'s `tool`. Risk: none
(`//scripts/parser:update_keyword_coverage_test`; `bazel cquery 'somepath(//scripts/parser:keyword_coverage,
//core:core)'` becomes empty).

**gen_differential** (`scripts/corpus/BUILD.bazel:188-204`). Now: `srcs = _GATE_DATA` (`:137-141`) with
`//core:stress_files` (all 202), `--inputs $(execpaths //core:stress_files)` (`:198-199`). Reads: 192 hand-written +
the committed 59/60/64 (`model.py:90-100` drops `GENERATED`, the 7 stress-kind files; `DENSE_DIR` unset), 31 project
files, `queries.pure`, the committed layout (`model.py:52-55`). Unread: committed 92-98 (11.5 MB). Change:
`core/BUILD.bazel` after `:767`:
```starlark
# the committed dense files (59, 60, 64): the differential reads them; the stress generators read gen_dense's outputs
filegroup(
    name = "stress_dense",
    srcs = ["src/test/resources/stress/" + f for f, g in STRESS_GENERATED.items() if g == "dense"],
    visibility = ["//scripts/corpus:__pkg__"],
)
```
and in `gen_differential`: `srcs = ["queries.pure", "//core:src/test/resources/com/legend/integration/stress-layout.json",
"//core:stress_dense", "//core:stress_sources"] + _LINKED`, `args = ["--out", "$(RULEDIR)/diff", "--inputs",
"$(execpaths //core:stress_sources)", "$(execpaths //core:stress_dense)"] + [...linked...]`, plus `testonly = True`
(§3). `_GATE_DATA` stays for the gates (they measure every stress file). Optional: `py_library(name =
"corpus_differential", 16 modules)` for `:differential` (`:61-65`). Risk: none (`cmp -r` of `bazel-bin/scripts/corpus/diff`
before/after; `//core:corpus_differential_test`).

**zone_jvm** (`wasm/BUILD.bazel:130-136`, "narrowed to LiteralSpelling"). Now: `zone_main` (`:85-91`) deps
`:boundary` → `//core:plan_side` (26 jars). Reads: `Wasm.zoneProbe` (`Wasm.java:405-411`) = a try/catch around
`LiteralSpelling.inZone` (`core/.../lowering/LiteralSpelling.java:714`). Change: `ZoneMain.java:33` calls a private
`answer(iso, zone)` that mirrors `Wasm.java:406-410` (`try { return LiteralSpelling.inZone(...); } catch
(RuntimeException e) { return "ERR " + e.getClass().getName() + ": " + e.getMessage(); }`); `zone_main` deps
`["//core:lowering"]` (15 jars); javadoc `:3-11`. Alternative keeping one source: a `ZoneProbe` class in its own
library on `//core:lowering`, used by both `Wasm.zoneProbe` and `ZoneMain` (touches the shipped WASM module). Risk: low
(`//wasm:zone_test` compares the two).

### 5.4 Self-inputs (a committed output inside a library its own generator runs on)

| Generator | Self-input | Phase 1 |
|---|---|---|
| gen_fixtures, gen_manifest | `engine-grammar-fixtures.jsonl`, `corpus-manifest.tsv` in `pe_tests_lib` resources (`PEB:51`) | fixed (§5.1: own libraries, and both files out of `pe_tests_lib`'s resources) |
| gen_imports | committed NameResolver.java compiled into `//core` | fixed (§5.1, §6.6) |
| reachability_metadata | its JSON in `:server_lib` resources (`warehouse/BUILD.bazel:74-75`) | fixed (§5.2) |
| gen_dynafn, gen_natives, gen_engine_handlers, gen_prelude | their committed files compiled into or carried by `//core` | Phases 2, 5, 2, 4 |
| gen_claims | `native-claims.tsv` in `core_next`'s resources (`core/BUILD.bazel:861-864`) | Phase 5 |
| `//core:ladder_report`, `//spec:ratchets`, `//pct:ratchets`, `//parser-equivalence:ratchets`, gen_roster | pins/ratchets in their test libraries | set aside (measurements) |

---

## 6. Upstream-only small generators

Today every committed upstream record's writer is in `//:update_generated` and its diff test in `//:generated`
(`BUILD.bazel:80-126`); that stays until Phase 7 (`//:update_upstream`, the seal). "Two runs, same bytes" on macOS is
already shown for today's six outputs (the two new files, CoreImports.java and pmcd-reachable.tsv, need their own): `runs/homework/base.sha` and `det.sha` (a from-scratch rebuild in a fresh
output base) are identical, 11 of 11 (`docs/UPSTREAM_ONLY_HOMEWORK_2026_10_05.md` §0). Each change below must keep
that, and Linux/Windows come from CI's checks lane (3 platforms) wherever the diff test is in `//:generated`.
Re-execution recipe for a local two-run check: build, copy the output, delete it from `bazel-bin`, rebuild with
`--disk_cache=`, `cmp`.

| # | Generator | Beyond upstream today | Change | Wiring after | Determinism check |
|---|---|---|---|---|---|
| 6.1 | gen_fixtures | `//core`, `pe_tests_lib` (classes and its own committed output), the pure tree; harvest code and the `# engine=` constant are generator code (R3 §3) | §5.1 | unchanged: `//parser-equivalence:update_generated_1` / `_1_test` (`PEB:349-358`) | `_1_test` on 3 platforms; local two-run |
| 6.2 | gen_manifest | `//core` (only `Diagnostics`, inert: R3 §4), `pe_tests_lib` | §5.1 | unchanged: `_0` / `_0_test` | as 6.1 |
| 6.3 | vocab | `//core`, `//base`, `//json` on the scanned class path (contribute nothing) | §5.1 | unchanged: `//tools/engine-runner:update_vocab` / `_test` (`:71-77`) | `update_vocab_test` on 3 platforms (also one Generate action now, not two) |
| 6.4 | ref_imports | nothing (deps `_REFERENCE_JARS` only) | commit it (below) | new manual writer, not in the roots in Phase 1 | macOS two-run done (`64c4d878…` both); Linux/Windows open (§7) |
| 6.5 | pmcd_reachability_census | our roster (`-Dpe.roster` = gen_roster, which reads our test trees), `pe_tests_lib`, both trees, fixtures, pins (unread) | split (below) | upstream half in `//parser-equivalence:update_generated` (`_2`); worklist manual | `_2_test` on 3 platforms; local two-run |
| 6.6 | gen_imports / CORE_IMPORTS | the host file NameResolver.java (E1) | whole generated file (below) | `//core:update_generated_2` / `_2_test` (`core/BUILD.bazel:782-787`) | `_2_test` on 3 platforms; local two-run |

### 6.4 ref_imports, committed
- Output today: `bazel-bin/tools/reference/ref-imports.tsv`, 1,412 lines, 1,619,993 bytes (`sourceId \t pkg,pkg,...`),
  from `TreeMap<String, TreeSet<String>>` (`RefImports.java:26,35`).
- **Required code fix:** `RefImports.java:41-42` writes with `PrintWriter.println`, the platform line separator (CRLF on
  Windows), so one committed copy cannot match on all three. Write `e.getKey() + "\t" + ... + "\n"` with `print`.
- `tools/reference/BUILD.bazel`, after `:109`:
  ```starlark
  # the reference compiler's import groups at the pinned release, committed (GENERATORS §2 row 9). Manual: the report
  # needs the 27-module closure compiled (memory_mb above); the bump regenerates it from Phase 7 (//:update_upstream).
  write_source_files(
      name = "update_ref_imports",
      testonly = True,
      diff_test_failure_message = "{{DEFAULT_MESSAGE}}\nref-imports.tsv is the reference compiler's import groups at the pinned release -- regenerate: bazel run //tools/reference:update_ref_imports",
      files = {"ref-imports.tsv": ":ref_imports"},
      tags = ["manual"],
  )
  ```
  Committed at `tools/reference/ref-imports.tsv`. Not in `//:update_generated`: that root is not manual, so listing it
  would make `bazel build //...` run this 8 GB action (see §7's pull-forward option).
- Measure its heap (`-Xlog:gc`, as `ref_dump`'s `:78` note): `memory_mb = 8192` (`:105`) is unmeasured; `ref_dump`
  peaks at 3,072 MB live over the same closure. At ≤4096 its diff test could join `//:generated` (the checks lane runs
  on 7 GB macOS runners).
- `tools/reference/README.md:94-95`: the report is now committed at `tools/reference/ref-imports.tsv`.

### 6.5 The reachability census, split
- **Upstream half, committed.** New `parser-equivalence/src/test/java/com/legend/equivalence/PmcdReachability.java`:
  `PmcdReachabilityCensus.java:44-130` (subtype edges from every listed `legend-engine` jar's
  `org/finos/legend/engine/protocol/` classes and from `PureProtocolExtensionLoader`, then the BFS from
  `PureModelContextData`), dropping the unused `tagToClass`, counting the classes and jars it skips (silent today,
  `:76-82`, `:116-118`), writing to `{OUT}` with `'\n'` (not `println`): a header line (`# reachable from
  PureModelContextData over the engine's protocol jars: N classes; U unloadable classes and J unreadable jars skipped`)
  then the sorted class names (783 at the last run).
  ```starlark
  legend_java_library(name = "pmcd_reachability_lib", nullaway = False, testonly = True,
      srcs = ["src/test/java/com/legend/equivalence/PmcdReachability.java"],
      runtime_deps = _ENGINE_EXTENSIONS, deps = ["//testing"] + _ENGINE_COMPILE)
  # the jars it walks are exactly its class path (the scope: _ENGINE_COMPILE + _ENGINE_EXTENSIONS, pe_tests_lib's)
  java_jars(name = "pmcd_jars_exec", testonly = True, exec_paths = True, deps = [":pmcd_reachability_lib"])
  filegroup(name = "pmcd_jar_files", testonly = True, srcs = [":pmcd_jars_exec"], output_group = "jars")
  java_run(
      name = "pmcd_reachability",
      testonly = True,
      srcs = [":pmcd_jar_files", ":pmcd_jars_exec"],
      outs = ["generated/pmcd-reachable.tsv"],
      arguments = ["{OUT}"],
      jvm_flags = ["-Dlegend.engine.jars=$(execpath :pmcd_jars_exec)"],
      main_class = "com.legend.equivalence.PmcdReachability",
      memory_mb = 2048,
      mnemonic = "Generate",
      deps = [":pmcd_reachability_lib"],
  )
  ```
  Same `legend-engine` jar set as today's `:engine_jars_exec` (pe_tests_lib's external closure; the census filters to
  paths containing `legend-engine`, `PmcdReachabilityCensus.java:50`), so the same reachable set. Committed at
  `parser-equivalence/pmcd-reachable.tsv` (package root, outside `src/test/resources`, so no test library carries it),
  added as a third entry of `//parser-equivalence:update_generated`'s `files` (`PEB:353-356`): writer `_2`, diff test
  `_2_test`, already in both roots (`BUILD.bazel:90,117`).
- **Our half, on demand.** New `PmcdWorklist.java` (`PmcdReachabilityCensus.java:132-162` over the committed list):
  ```starlark
  legend_java_library(name = "pmcd_worklist_lib", nullaway = False, testonly = True,
      srcs = ["src/test/java/com/legend/equivalence/PmcdWorklist.java"], deps = ["//testing"])
  java_run(
      name = "pmcd_worklist",
      testonly = True,
      srcs = ["pmcd-reachable.tsv", ":gen_roster"],
      outs = ["pmcd_worklist/pmcd-worklist.txt", "pmcd_worklist/run.log"],
      arguments = ["{OUT_DIR}"],
      jvm_flags = ["-Dpe.reachable=$(execpath pmcd-reachable.tsv)", "-Dpe.roster=$(execpath :gen_roster)"],
      main_class = "com.legend.equivalence.PmcdWorklist",
      mnemonic = "Measure",
      tags = ["manual"],
      deps = [":pmcd_worklist_lib"],
  )
  ```
  Its three `@@` sections are today's report's. `_REPORTS` row `PEB:422` deleted; `:diagnostics_reports` srcs
  (`:453`) `+ [":pmcd_worklist"]`; delete `PmcdReachabilityCensus.java`. (`scripts/census_gate.py:52` is Maven-era
  history.)
- Check: `bazel run //parser-equivalence:update_generated_2`, then `_2_test`; the worklist's lines equal the old report
  built at the parent commit (`bazel-bin/parser-equivalence/pmcd_reachability_census/pmcd-reachability-census.txt`).

### 6.6 CORE_IMPORTS in its own generated file
- **New file** `core/src/main/java/com/legend/compiler/CoreImports.java`, generated whole, in `//core:compiler`'s
  glob (`core/BUILD.bazel:117-121`), so no new library or layer:
  ```java
  // GENERATED by //spec:gen_imports from legend-engine's CompileContext.META_IMPORTS: do not edit.
  // Regenerate: bazel run //:update_generated
  package com.legend.compiler;

  import java.util.List;

  /**
   * The implicit import group every element resolves through, walked FIRST-MATCH: the engine's sequence
   * ({@code CompileContext.META_IMPORTS}: legend-pure's {@code system::imports::coreImport} plus the packages the
   * engine adds, at the engine's positions). The order is semantic; {@code CoreImportsParityTest} holds it.
   */
  public final class CoreImports {

      private CoreImports() {}

      public static final List<String> SEQUENCE = List.of(
              "meta::pure::metamodel",
              ...
              "meta::pure::precisePrimitives");
  }
  ```
  No counts or release in the template (inputs: CompileContext.java alone). LF only (a text block or `'\n'`).
- **Generator** `spec/src/gen/java/com/legend/generators/ImportsGenerator.java`: usage `ImportsGenerator
  <CompileContext.java> <output>` (`:35-44`); `metaImports` (`:47-59`) kept (the test calls it); `generate(imports,
  nameResolverJava)` (`:61-74`) and `OPEN` (`:31`) replaced by `render(List<String>)` emitting the file above; refuse
  an empty list; javadoc `:15-28`.
- **`spec/BUILD.bazel`**: `gen_imports` (`:330-347`) `srcs = [_ENGINE_COMPILE_CONTEXT]`, `outs =
  ["generated/CoreImports.java"]`, `arguments = ["$(execpath %s)" % _ENGINE_COMPILE_CONTEXT, "{OUT}"]`, `deps =
  [":imports_generator"]` (§5.1); comment `:330`; `gen_claims` override `:505` →
  `"com/legend/compiler/CoreImports.java=$(execpath :gen_imports)"`.
- **`core/BUILD.bazel`**: `_GENERATED` key `:702` → `"src/main/java/com/legend/compiler/CoreImports.java":
  "//spec:gen_imports"` (same position, so still writer `_2`; `exports_files` `:710-715` follows the keys); comment
  `:708-709` (only DynaFn.java and Pure.java are spliced now); `core_next` exclude `:849` → `CoreImports.java`.
- **Readers** (drop the field from NameResolver; the IN_FLIGHT note says the readers read the new file):
  - `core/src/main/java/com/legend/compiler/NameResolver.java:206-245`: delete the javadoc and constant; `:376`,
    `:713`: `CORE_IMPORTS` → `CoreImports.SEQUENCE` (same package).
  - `core/src/main/java/com/legend/compiler/BareNames.java:27` (`{@link CoreImports#SEQUENCE}`), `:60`.
  - `core/src/main/java/com/legend/server/DiagramService.java:85` (comment).
  - `spec/src/gen/java/com/legend/generators/PreludeGenerator.java:321`, `:475` (+ `import
    com.legend.compiler.CoreImports;`; the `NameResolver` import `:9` stays for `:768`, `:874`).
  - `spec/src/test/java/com/legend/generators/CoreImportsParityTest.java:9` (import), `:23`, `:40` (javadoc),
    `:80-81`.
  - `tools/untangle/probe_counts.py:55-64` (path `.../compiler/CoreImports.java`, marker `SEQUENCE = List.of(`; it
    silently prints 0 packages if missed).
  - Docs: `docs/GATES.md:43` ("NameResolver.java's imports" → CoreImports.java), `tools/reference/README.md:98`.
- Unaffected, checked: `PlatformNamesGuardrailTest.functionFqnLiteralsOutsideTheCatalogsOnlyShrink` counts a single
  total (`:142`), and the 32 literals move file to file; the prelude's demand scan and native-claims' `also` column look
  for class and function FQNs, not packages; `ArchitectureTest` has no `CORE_IMPORTS` rule.
- Bootstrap order: generator + BUILD first, `bazel run //core:update_generated_2` writes the file (the generator no
  longer needs core to compile), then the reader edits, then build.

---

## 7. Order and risk

Commits, each green on its own (local checks named; `bazel build --nobuild --config=bazel10 //...` after every one):

| # | Commit | Local check | Risk |
|---|---|---|---|
| 1 | delete `migration_sizing` (§1.1) | gate 8 (`//parser-equivalence:parser_parity`, `pe_tests_lib` changed) | low |
| 2 | delete world 2 (§1.2: BUILD, probe branch, 2 pins) | `//core:census`, `//spec:spec_tests` | low |
| 3 | manual tags (§2) | `bazel query 'attr(tags, "\bmanual\b", ...)'`; `bazel build //spec:native_declarations` still builds | low |
| 4 | testonly (§3), incl. `offer_facts` and `//datacube:update_generated` | `//core:corpus_differential_test //datacube:wasm_differential_test //wasm:differential_test //wasm:zone_test //tools/java_run:pins_test //datacube:update_generated_tests` | medium (the testonly chain into the datacube writer) |
| 5 | PAR timestamps (§4) | two-run `cmp`; entry times; one PCT suite | medium (lanes 6/7/7p/9 rerun; content variance OPEN until the two-run) |
| 6 | gen_differential inputs (§5.3) | `cmp -r` of the diff tree; `//core:corpus_differential_test` | low |
| 7 | vocab off keyword_coverage; `token_dump` (§5.3, §5.1) | `update_vocab_test`, `update_keyword_coverage_test`; the cquery above empty | low |
| 8 | gen_dense/gen_stress (§5.2) | `//core:update_stress_corpus_tests` | low |
| 9 | datacube, engine-client, wasm narrowing + core visibility lines (§5.2, §5.3) | `//datacube:update_generated_tests //engine-client:update_generated_tests //wasm:zone_test //datacube:tests` | medium (10 visibility lines in `core/BUILD.bazel`) |
| 10 | reachability metadata (§5.2) | `update_reachability_metadata_test`; `bazel build //warehouse:server_native`; `//warehouse:tests_native` | medium (native image) |
| 11 | gen_dynafn pure tree (§5.1) | `//core:update_generated_0_test` | low |
| 12 | `pe_corpus`, gen_fixtures, gen_manifest, PE resources (§5.1) | `//parser-equivalence:update_generated_tests`, gate 8 | **high** (harvest class path) |
| 13 | pmcd split + `pmcd-reachable.tsv` (§6.5) | `_2_test`; worklist = old report; two-run | medium |
| 14 | ref_imports committed (§6.4: LF fix, heap measured, writer) | two-run `cmp`; `bazel test //tools/reference:update_ref_imports_test` | medium |
| 15a | `imports_generator` (§5.1) | `//core:update_generated_2_test` (same bytes) | low |
| 15b | CoreImports.java (§6.6), the announced core edit | the six `//core:update_generated_*_test` (only `_2`'s file is new), `//spec:spec_tests`, `//core:core_tests`, `//core:guardrails`, `//core:census` | **high** (core Java: every lane reruns) |

Then: `bazel test //:generated //gates:local`, `bazel build //...`; an audit agent's review of the Bazel changes
before any push (the Bazel program's rule); IN_FLIGHT on main updated first; a throwaway CI with every lane key on
Linux, macOS and Windows (15b changes core main, which every lane reaches; it also gives Linux/Windows proof for every
diff test in `//:generated`).

IN_FLIGHT gaps: the 2026-10-06 note (`docs/IN_FLIGHT.md:34-38`) names BUILD declarations in core/, spec/,
parser-equivalence/, pct/, scripts/corpus/, tools/, datacube/, engine-client/, legend-art/, warehouse/ and the
CORE_IMPORTS move. Add before landing: `wasm/` (`ZoneMain.java`, BUILD), `docs/` (`BUILD.bazel`, `GATES.md`),
`scripts/parser/`, the 10 core visibility lines, the two core test pins (§1.2), and the Java edits in `spec/`
(ImportsGenerator, PreludeGenerator, CoreImportsParityTest, EagerCorpusCompileProbe), `parser-equivalence/` (two new
programs, two deletions), `tools/par`, `tools/reference`, `tools/untangle`. pct/ and legend-art/ need no change (unless
the optional PAR test lands in pct/).

### Not settled, and what settles each
1. **ref_imports off macOS.** Its diff test is manual, so CI never runs it, and until Phase 7 a bump does not regenerate
   it. Settle: measure its heap; at ≤4096 put the writer in `//:update_generated` and the test in `//:generated`, or
   (pull-forward) tag `//:update_generated` `manual` now (it then stops building `//pct:ratchets` in `//...` too) and list
   the writer there; for Linux/Windows bytes, one throwaway CI with the test in a lane.
2. **gen_own_corpus_draft and its writer `manual`** sit in GENERATORS §5's set-aside bullet. Settle: the user.
3. **catalog_rules:** A (12 jars, no Java edit, narrows catalog_corpus too) or B (3 jars, splits `CatalogFacts.java`).
   Settle: the user.
4. **offer_facts:** five exact core deps (5 visibility lines, 24 jars) or `//core:plan_side` (1 line, 26 jars).
   Settle: the user (this list assumes exact).
5. **zone_jvm:** mirror the try/catch in ZoneMain (no product change) or a shared `ZoneProbe` class (one source, touches
   the WASM module). Settle: the user (this list assumes mirror).
6. **World 2:** delete the probe branch and move two pins, or the BUILD tuple only. Settle: the user (this list assumes
   both).
7. **CoreImports field name** `SEQUENCE` (R3's) or keep `CORE_IMPORTS`. Settle: the user.
8. **Where the two new committed reports live** (`parser-equivalence/pmcd-reachable.tsv`,
   `tools/reference/ref-imports.tsv`), and pmcd's diff test in the everyday `//:generated` until Phase 7's seal.
   Settle: the user.
9. **Engine-tree subsets** (gen_fixtures' 3 tier-2 modules, gen_dynafn's relationalStore, the spec roots): need new
   filegroups in `third_party/legend_engine_src.BUILD`, which re-extracts the archive and only shrinks sandbox inputs
   (the tree changes only in a bump). Deferred; settle if sandbox setup time shows up.
10. **PE test data** (`_FILE_DATA`'s unread `corpus-manifest.tsv`, `protocol-roster` in `_LEDGERS`, `PEB:142-157`):
    test inputs, not generators. Phase 8 unless the user wants it here.
11. **The PAR fix's permanent check** (the optional entry-time test in `//pct:pct_duckdb`). Settle: the user.
12. **`Bump.java:163-165`'s NEXT list** names NameResolver's file only by implication; it should name CoreImports.java
    and the two new records. Phase 7 rewrites the bump; fix now only if wanted.
