# G3 dossiers: the upstream-facing generators

Repo: `the build/rebuild checkout` (branch build/rebuild, HEAD `669b39ad1`). Paths are repo-relative.
Shorthands: `PE/` = `parser-equivalence/src/test/java/com/legend/equivalence/`, `PEB` = `parser-equivalence/BUILD.bazel`,
`CHB/` = `pct/src/test/java/org/finos/legend/lite/pct/channelb/`, `PURE/` = the pinned legend-pure source archive in the
output base (`external/+http_archive+legend_pure_src/legend-pure-core/legend-pure-m3-core/src/main/java/org/finos/legend/pure/m3/`).

**"aq"** = `env -C <repo> bazel aquery '<the 15 java_run labels>' --output=jsonproto` plus a separate
`bazel aquery '//scripts/parser:keyword_coverage'`, summarized per action by input category (script in the session scratchpad,
run 2026-10-05). In every java_run action: `@legend_engine_src//:tree` = 12,900 files, `@legend_pure_src//:tree` = 2,693 files,
the remote JDK 25 = 117 files (one of them `jrt-fs.jar`, counted in the jar totals below). "core jars" = the 31 jars
`//core:core` exports (`core/BUILD.bazel:210-214` `_CORE_TARGETS`, minus `//base` and `//json`, which are counted apart).

**Three facts that hold for every java_run generator below (and are not repeated):**
- **F1, how a generator reruns.** `java_run` takes the FULL transitive runtime jars of its deps as action inputs
  (`tools/java_run/defs.bzl:46,110`); there is no ijar cutoff, so any source edit anywhere in the closure reruns the action.
  The action's OUTPUT is content-addressed, so a downstream diff test reruns only if the bytes moved.
- **F2, the classpath guards are analysis-only.** Every java_run is reachable from `//gates:local` through
  `//tools/guards:classpath_test → classpath_reports → <pkg>:guard_classpaths` (query: `somepath(//gates:local, <gen>)`
  returns this path for all seven manual generators). `classpath_report` reads `JavaRuntimeClasspathInfo` at analysis and
  writes a TSV with `ctx.actions.write` (`tools/guards/classpath.bzl:38-50`); `markdown_report` likewise
  (`tools/guards/markdown.bzl:9-18`). So these edges force ANALYSIS of manual generators, never their execution.
- **F3, java_run pins** `-Duser.timezone=GMT -Duser.language=en -Duser.country=US -Dfile.encoding=UTF-8
  -Djava.io.tmpdir=<name>_tmp` on every action (`tools/java_run/defs.bzl:85-91`), and `-Xmx`+a resource_set only when
  `memory_mb` is set (`:92-99,114`). `program_jvm_flags` adds a second, redundant `-Duser.timezone=GMT`
  (`tools/generators/defs.bzl:35`).

**The root writer and the bump.** `//:update_generated` (`BUILD.bazel:105-125`) is NOT manual; it lists
`//parser-equivalence:update_generated`, `//docs:update_generated`, `//parser-equivalence:update_ratchets`,
`//pct:update_ratchets`, `//scripts/parser:update_keyword_coverage`, `//tools/engine-runner:update_vocab` among its
`additional_update_targets`, whose `default_runfiles` it merges into its own (bazel_lib `write_source_file.bzl:440-447`). So
`bazel build //...` builds it, and with it the MANUAL `//pct:ratchets`
(`somepath(//:update_generated, //pct:ratchets)` = `//:update_generated //pct:update_ratchets //pct:ratchets`). The bump
(`tools/bump/Bump.java:20-51,147-156`) runs `bazel run //:update_generated` (phase 2) then `bazel test //...` (phase 3), and its
"judgement half" message names `corpus-manifest.tsv / protocol-roster.tsv / the fixture snapshot` as the diff to read
(`Bump.java:160-166`). `//:generated` (`BUILD.bazel:79-97`) is CI's checks lane and the first member of `//gates:local`
(`gates/BUILD.bazel:15`).

---

## 1. //parser-equivalence:gen_fixtures

1. **Identity.** `java_run` (rule `_java_run`), `PEB:280-297`. Program `com.legend.equivalence.harvest.FixtureHarvestGenerator`
   (`PE/harvest/FixtureHarvestGenerator.java:29`), with `FixtureHarvest` (`PE/harvest/FixtureHarvest.java`), `FixtureRecorder`
   (`PE/harvest/FixtureRecorder.java`) and three recording shims in the engine's namespace
   (`parser-equivalence/src/test/java/org/finos/legend/engine/language/pure/{compiler,grammar}/test/*.java`, listed `PEB:37-42`).
2. **What it computes.** It harvests "corpus tier C6": every Pure text legend-engine's OWN grammar and compiler tests assemble.
   It runs every `Test*` class of the engine's published grammar and compiler tests-jars (tier 1), then compiles and runs the
   test sources of three engine extension modules that publish no tests-jar (tier 2), with our shims standing in for the
   engine's test base classes so every `test(...)` call records its source text instead of asserting. The dump is deduplicated
   by text and written as JSONL with a `# engine=<release>` header.
3. **Why it exists.** The harvest was introduced as `ZEngineFixtureHarvest` in `49386fed7` (2026-08-11, "Harvest by execution:
   the engine's own grammar tests are now our fixture corpus — 818 fixtures, 7 byte-DIFFs and 25 gaps found day one"); it became
   this Bazel generator in `998f1a41c` (2026-09-23). It feeds gate 8's corpus (the per-production coverage the static tiers miss,
   `PE/Corpus.java:257-262`) and the three other corpus generators (gen_manifest, gen_roster, the censuses read its OUTPUT via
   `-Dlegend.engine.fixtures`, `PEB:299-301`). Reason still live.
4. **Inputs, declared** (aq: 16,152 inputs).
   - Upstream archives: `UPSTREAM_TREES` (`PEB:283`): engine pom + tree (12,900), pure pom + tree (2,693).
   - Maven: `deps = [":harvest_lib"]` (`PEB:296`) = `:harvest_shims` + `:pe_tests_lib` + the grammar and compiler tests-jars +
     json-unit (`PEB:247-258`); 440 jars: 404 external (@maven_upstream, incl. the two tests-jars) + jrt-fs, 36 ours = 31 core
     jars, `//base`, `//json`, `//testing`, `pe_tests_lib`, `harvest_shims`.
   - Other outputs: `:harvest_tests_jars` (list file) + `:harvest_tests_jar_files` (`PEB:284-286`), `//tools:oracle-pins.env`
     (copy of `@oracle_pins`, `tools/BUILD.bazel:7-12`).
   - Flags: `program_jvm_flags(..., pure = False)` (engine root only), `-Doracle.pins`, `-Dlegend.harvest.jars` (`PEB:290-292`).
5. **Inputs, actually read.**
   - `-Dlegend.harvest.jars` list → the 2 tests-jars, enumerated by `JarFile` (`FixtureHarvestGenerator.java:41-48`,
     `FixtureHarvest.java:119-136`).
   - `-Dlegend.engine.root` → the src/test/java of exactly three engine modules (`FixtureHarvest.java:45-48,168-176`), compiled
     with the system javac against `java.class.path` (`FixtureHarvestGenerator.java:51,56`; `FixtureHarvest.java:178-180`).
   - `-Doracle.pins` → `OraclePins.engineRelease()` for the header (`FixtureRecorder.java:39-41`; `PE/OraclePins.java:24-44`).
   - System property `fixture.dump` it sets itself (`:36`); temp dir under the action's scratch (`:33`, F3).
   - The classpath: engine test classes and whatever engine extensions ServiceLoader finds on it (the grammar extension
     runtime_deps of `pe_tests_lib`, `PEB:70-79`), which decide what the engine tests can parse.
   - **Our code it truly calls:** the harvest classes, the 3 shims, `OraclePins`, `com.legend.testing.ProgramPaths`/`Runfile`,
     and the constant `Corpus.FIXTURE_HEADER_PREFIX` (`PE/Corpus.java:157`, a compile-time `static final String`, inlined by
     javac). The shims reference only `FixtureRecorder` (grep of the 3 files). **No core class.**
   - Over-declared: the whole pure tree (2,693 files; `pure = False` already says so); 12,897 of the engine tree's files
     (only three modules' test sources are read); all 31 core jars, `//base`, `//json`; the rest of `pe_tests_lib`.
   - Under-declared: none found. Caveat: an engine test that reads a file from disk instead of its classpath would throw inside
     the sandbox and be counted `invoke-threw`, not fail the action (`FixtureHarvest.java:96-101`). OPEN: whether any fixture is
     lost that way; settle by comparing the receipt counts (`@@ tier 1/2` lines) with an unsandboxed run.
6. **Outputs.** `parser-equivalence/generated/engine-grammar-fixtures.jsonl`: header `# engine=<release>`, then one JSON row per
   distinct source `{source, expectedError?, kind, origin}` in harvest order (`FixtureHarvest.java:211-234`).
7. **Committed?** Yes: `parser-equivalence/src/test/resources/engine-grammar-fixtures.jsonl`. Writer
   `//parser-equivalence:update_generated` (`PEB:349-358`; also in `//:update_generated`). Diff test
   `//parser-equivalence:update_generated_1_test`, suite `:update_generated_tests`, in `//:generated` (`BUILD.bazel:89`).
8. **Who consumes it.** The COMMITTED copy: gate 8 `//parser-equivalence:parser_parity` and `:diagnostics` (data + 
   `-Dpe.engine.fixtures.rlocation`, `PEB:154-157,165`) through `Corpus.engineFixtures()` (`PE/Corpus.java:179-235`), used by
   `CorpusSweepTest`, `ComposerParityTest`, `SectionParseSentinelTest`, `GrammarCoverageCensusTest`; the binaries
   `parse_speed_benchmark`, `z_fixture_adjudication_probe`. It also rides in `pe_tests_lib`'s resources (`PEB:51`, glob
   `src/test/resources/**`), read by nobody from there. The generator OUTPUT: `gen_manifest`, `gen_roster` and the four
   censuses (`PEB:313,337,438`). Humans: the bump's judgement list (`Bump.java:164`). No product code.
9. **Determinism.** Designed for it: classes and methods run in name order because `getMethods()` has no order and a no-op bump
   once rewrote 774 snapshot lines (`FixtureHarvest.java:32-37,50-60,132-134,149`); dedup keeps first by text. The diff test runs
   in CI's checks lane on all three platforms (`.github/workflows/gates-run.yml:51`), so a byte drift would be red. Nothing reads
   the clock; the temp dir's random name never reaches the output.
10. **Cost.** No `memory_mb` (JVM default heap, no resource_set). One JVM that runs every test method of two tests-jars, plus
    in-process javac over three modules (whole, then per file on failure, `FixtureHarvest.java:161-199`). No DB, no server.
11. **What reruns it today.** Any edit to any core main source (F1;
    `somepath(//parser-equivalence:gen_fixtures, //core:exec)` = `gen_fixtures → harvest_lib → pe_tests_lib → //core:core →
    //core:exec`); any edit under `parser-equivalence/src/test/**` including the committed outputs themselves
    (`somepath(gen_fixtures, //parser-equivalence:src/test/resources/engine-grammar-fixtures.jsonl)` =
    `gen_fixtures → harvest_lib → pe_tests_lib → …jsonl`: its own committed output is an input); `//testing`, `//base`,
    `//json`; any @maven_upstream jar; either upstream tree; the oracle pins; the JDK.
12. **What SHOULD rerun it.** An upstream bump (engine release, hence tests-jars, tree, pins). Plus edits to the harvest code
    and shims themselves.
13. **Who runs it today.** `bazel build //...` (not manual; CI build lane, `gates-run.yml:63`); `//gates:local` and CI checks
    via `//:generated` (`somepath(//gates:local, gen_fixtures)` = `local → //:generated →
    parser-equivalence:update_generated_tests → update_generated_1_test → gen_fixtures`). Gate 8 (`parser_parity`) does not run it: tests read the committed copy. The bump, phase 2 (`//:update_generated`) and phase 3.
    By hand: `bazel run //:update_generated` (`README.md:294`, `docs/GATES.md:47`, the error at `PE/Corpus.java:199-201`).
14. **Recommendation: COMMITTED-UPSTREAM.** The tests need a committed snapshot (re-harvesting per test run would put the
    tests-jars in every gate-8 run); its only true trigger is the release. `manual`: yes. Writer: the bump-only update
    (`//:update_upstream` in the design, `BUILD_REBUILD_DESIGN_2026_10_05.md:171-183,228-231`). Diff test: the bump's check (and
    may stay in `//:generated` cheaply only once narrowed, since it then reruns on nothing else). Narrowing: a `:harvest_lib`
    that depends on `:harvest_shims` + a tiny `:pe_pins` library (`OraclePins`, the header constant) + `//testing` + the
    @maven_upstream grammar/compiler jars, tests-jars, json-unit and the grammar-extension runtime_deps now on `pe_tests_lib`
    (`PEB:70-79`; they change what the engine tests parse, so they must stay); no `//core`, no `pe_tests_lib`. Declare only the
    engine tree (drop the pure tree) and, if wanted, a filegroup of the three tier-2 modules' `src/test/java`.
15. **Open questions.** (a) Do sandboxed engine tests lose fixtures through undeclared file reads (see 5)? Settle with an
    unsandboxed comparison run. (b) Would dropping `pe_tests_lib` from the harvest classpath change which extensions
    ServiceLoader finds and hence the output? Settle by building the narrowed library and diffing the output.

## 2. //parser-equivalence:gen_manifest

1. **Identity.** `java_run`, `PEB:304-318`. Program `com.legend.equivalence.ManifestGenerator` (`PE/ManifestGenerator.java:27`).
2. **What it computes.** The whole parser-equivalence corpus as data: one row per distinct source text (SHA-256, tier, id), in
   corpus order. The corpus is every `.pure` in both upstream trees, the Pure snippets embedded in their Java tests, the engine
   fixture snapshot, and Pure documents in engine `.txt` resources, deduplicated by text (`PE/Corpus.java:242-283`).
3. **Why it exists.** `0e2bd8938` (2026-08-12, "corpus as data — dedupe + SHA-256 manifest"): so corpus drift is "a reviewed
   diff, never a silent renumbering", enforced then by `CorpusManifestTest`. That test was deleted in `cf911b24f` (2026-10-04,
   P2-19) because the `//:generated` diff test already guards the file. Under Bazel the trees are pinned by archive integrity
   (`release.MODULE.bazel:28,31`), so the original "stale checkout" failure mode is gone; what remains is the legible record of
   how the corpus moved in a bump (`Bump.java:163-164`).
4. **Inputs, declared** (aq: 16,147). Upstream trees (15,593 + 2 poms); `:gen_fixtures` output; oracle pins; `deps =
   [":pe_tests_lib"]`: 435 jars (400 external + jrt-fs; 35 ours = 31 core, base, json, testing, pe_tests_lib).
5. **Inputs, actually read.** Both trees walked whole for `.pure` (excluding `/target/`), engine+pure `src/test/**.java` for
   inline snippets, engine `.txt` (`PE/Corpus.java:59-82,90-113,243-265`; `PE/InlineSnippets.java:57-107`); the fixtures via
   `-Dlegend.engine.fixtures` (`Corpus.java:179-188`); the pins to check the fixture header (`Corpus.java:207-219`);
   `-Dlegend.diagnostics` (`Corpus.java:275`, `core/src/main/java/com/legend/diagnostics/Diagnostics.java:33`). **Our code it
   truly calls:** `ManifestGenerator`, `Corpus`, `InlineSnippets`, `OraclePins`, `//testing` `ProgramPaths`, and one core
   class, `com.legend.diagnostics.Diagnostics` (`//core:diagnostics`, deps `//base` only). It parses nothing: no engine parser,
   no lite parser. Jackson (for the fixture rows) is the only third-party code used.
   Over-declared: 30 of the 31 core jars, every engine/pure Maven jar except Jackson. Under-declared: none.
6. **Outputs.** `parser-equivalence/generated/corpus-manifest.tsv`: `sha256 \t tier \t id` per distinct source (9,134 lines).
7. **Committed?** Yes: `parser-equivalence/src/test/resources/corpus-manifest.tsv`; writer `//parser-equivalence:update_generated`
   (`PEB:349-358`); diff test `:update_generated_0_test` in `:update_generated_tests` in `//:generated`.
8. **Who consumes it.** No test or program reads it: `grep -rn corpus-manifest` over all `.java/.py/.bazel` finds only
   `PEB`, `ManifestGenerator.java` and `Bump.java`. It is passed as data to `parser_parity`, `:diagnostics`, the probes and the
   benchmark (`_FILE_DATA`, `PEB:154-157`) and rides in `pe_tests_lib`'s resources; none of them opens it (over-declared there).
   Its consumer is a human reading the bump's diff (`Bump.java:163-164`).
9. **Determinism.** Corpus order is fixed by a slash-path sort (`Corpus.java:71-78`, written after a Windows mis-ordering) and
   InlineSnippets' sorted walks; no clock. Diff-tested on three platforms in CI's checks lane.
10. **Cost.** Default heap; reads ~16 k files and hashes ~9 k texts. No JVM beyond the one action, no DB.
11. **What reruns it today.** Every core edit (`somepath(//parser-equivalence:gen_manifest, //core:exec)` =
    `gen_manifest → pe_tests_lib → //core:core → //core:exec`); every PE test edit including its own committed output
    (`somepath(gen_manifest, //parser-equivalence:src/test/resources/corpus-manifest.tsv)` = `gen_manifest → pe_tests_lib →
    …corpus-manifest.tsv`); gen_fixtures' output; @maven_upstream; trees; pins.
12. **What SHOULD rerun it.** An upstream bump; edits to `Corpus.java`/`InlineSnippets.java` (the corpus definition).
13. **Who runs it today.** `bazel build //...`; `//gates:local` and CI checks via `//:generated`
    (`local → //:generated → update_generated_tests → update_generated_0_test → gen_manifest`); the bump (phases 2, 3); by hand
    `bazel run //:update_generated` (`README.md:294`).
14. **Recommendation: COMMITTED-UPSTREAM.** A bump-review record only. `manual`: yes. Writer: bump-only update. Diff test: the
    bump's check (a Corpus/InlineSnippets edit is rare and can run the bump check). Narrowing: a corpus library
    (`Corpus`, `InlineSnippets`, `ModuleFiles`, `OraclePins`, `ManifestGenerator`) with deps `//core:diagnostics`, `//testing`,
    `@maven_upstream//:com_fasterxml_jackson_core_jackson_databind`; no `//core`, no engine jars. Drop it from `_FILE_DATA`.
    Option for the user: since nothing reads it, it could also be DEAD if the bump diff of the fixture snapshot is judged enough.
15. **Open questions.** Is the manifest still wanted as a bump record (the user's call; nothing in the build needs it)?

## 3. //parser-equivalence:gen_roster

1. **Identity.** `java_run`, `PEB:322-345` (`visibility = ["//docs:__pkg__"]`). Program `com.legend.equivalence.RosterGenerator`
   (`PE/RosterGenerator.java:49`).
2. **What it computes.** The protocol-type roster: every `@JsonSubTypes` tag declared by classes under
   `org/finos/legend/engine/protocol/` in the engine jars, plus the extension registry, one row per (tag, class), marked COVERED
   if the ENGINE's parser, run over the engine corpus, the fixture snapshot and our own test snippets (core, spec, pct), emits a
   protocol JSON containing that `_type` (`RosterGenerator.java:58-176`).
3. **Why it exists.** Bazel-era form of `ProtocolRosterCensusTest`; this program since `998f1a41c` (2026-09-23); that test was
   deleted in `cf911b24f`. Purpose stated in its header: "a bump that adds or removes a tag, or moves a tag between COVERED and
   UNCOVERED, is a reviewed diff" (`RosterGenerator.java:39-43`). It also feeds `pmcd_reachability_census` (`PEB:440`).
4. **Inputs, declared** (aq: 17,598). Trees; `:engine_jars_exec` + `:engine_jar_files` (the jars list, `PEB:97-109`);
   `:gen_fixtures`; `//core:srcs`, `//pct:srcs`, `//spec:srcs` (every file under each `src/**`: 759 core/src/main, 573 core/src/test,
   1 core/src/scale, 4 pct/src/main, 33 pct/src/test, 11 spec/src/gen, 69 spec/src/test); oracle pins; 435 jars as gen_manifest.
5. **Inputs, actually read.** The jar list `-Dlegend.engine.jars` (opened with `JarFile`, classes loaded,
   `RosterGenerator.java:66-98`); `PureProtocolExtensionLoader.extensions()` (classpath, `:99-103`); `Corpus.all()` +
   `Corpus.engineFixtures()` (trees, fixtures output, pins; `:112-113`); `InlineSnippets.extract(module, …, OWN_DECL)` for
   core/spec/pct, which walks `<module>/src/test` and keeps `.java` only (`:114-117`; `PE/InlineSnippets.java:112-125`;
   `PE/ModuleFiles.java:31-46`). The parse is the ENGINE's `PureGrammarParser` + `ObjectMapperFactory` (`:107-109`). **Our code
   it truly calls:** `RosterGenerator`, `Corpus`, `InlineSnippets`, `ModuleFiles`, `OraclePins`, `//testing`
   (`ProgramPaths`, `SourceFiles`), `//core:diagnostics`. **No lite parser** (design agrees, `BUILD_REBUILD_DESIGN…md:193`).
   Over-declared: `core/src/main` (759 files: `somepath(//parser-equivalence:gen_roster, //core:src/main/java/com/legend/exec/Executor.java)`
   = `gen_roster //core:srcs …Executor.java`), `core/src/scale`, `pct/src/main`, `spec/src/gen`, every non-`.java` test file;
   30 core jars. Under-declared: none.
6. **Outputs.** `parser-equivalence/generated/protocol-roster.tsv`: a 5-line header, then `tag \t class \t COVERED|UNCOVERED`.
7. **Committed?** Yes: `docs/protocol-roster.tsv`; writer `//docs:update_generated` (`docs/BUILD.bazel:11-19`; in
   `//:update_generated`); diff test `//docs:update_generated_test`, suite `//docs:update_generated_tests` in `//:generated`
   (`BUILD.bazel:87`).
8. **Who consumes it.** The committed copy is declared as ledger `protocol-roster` for `parser_parity`/`:diagnostics`/probes
   (`PEB:142-152,154,159-161`), but no test opens it: `CorpusSweepTest.readAllowlist` is called only for four other ledgers
   (`PE/CorpusSweepTest.java:166-172`), and `grep protocol-roster` finds no other Java reader. The OUTPUT is read by
   `pmcd_reachability_census` (`-Dpe.roster`, `PEB:440`; `PE/PmcdReachabilityCensus.java:134-136`). Maven-era scripts
   `scripts/census_gate.py` and `scripts/corpus/coverage.py:47` name a different, dead path (`parser-equivalence/target/…`;
   `cf911b24f` says census_gate.py "cannot run"). Humans: the bump's judgement list (`Bump.java:164`).
9. **Determinism.** TreeMap/TreeSet throughout; a tag maps to every declaring class precisely because first-seen depended on
   classpath order (`RosterGenerator.java:60-64`). Diff-tested on three platforms.
10. **Cost.** Default heap; loads every protocol class of ~400 jars and parses ~9 k corpus sources plus our snippets with the
    engine parser. No DB.
11. **What reruns it today.** Every core MAIN source edit twice over: through `pe_tests_lib → //core:core` (F1) and through the
    declared `//core:srcs` files; every edit under core/pct/spec `src/**`; PE test edits; fixtures; @maven_upstream; trees; pins.
12. **What SHOULD rerun it.** An upstream bump; an edit to a `.java` file under `core/src/test`, `spec/src/test` or
    `pct/src/test` (it can move a tag to COVERED); edits to Corpus/InlineSnippets/RosterGenerator.
13. **Who runs it today.** `bazel build //...`; `//gates:local` and CI checks via `//:generated` (`local → //:generated →
    //docs:update_generated_tests → //docs:update_generated_test → gen_roster`); the bump; by hand `bazel run //:update_generated`
    (header line, `RosterGenerator.java:43`).
14. **Recommendation: COMMITTED-SOURCE (mixed: upstream + our test trees).** Its everyday trigger is our test `.java`; its big
    trigger is the bump. `manual`: yes. Writer: the bump's update (the design's group B, `…DESIGN…md:186-197`), with the diff
    test in the everyday gate only after narrowing (it then reruns on test-source edits and the release, which is right).
    Narrowing: deps = the corpus library of §2 + `@maven_upstream` grammar, protocol_pure, shared_core, Jackson annotations and
    the grammar-extension runtime_deps (they define the roster); no `//core` beyond `:diagnostics`; srcs `//core:test_java`,
    `//pct:test_java`, `//spec:test_java` (all exist, `PEB:116-125`) instead of the three `:srcs`. Drop `protocol-roster` from
    `_LEDGERS`.
15. **Open questions.** Has a test-tree edit ever moved a tag to COVERED (i.e. is the everyday trigger real)? Settle with
    `git log -p docs/protocol-roster.tsv` against the commits' touched paths.

## 4. //parser-equivalence:ratchets

1. **Identity.** `java_run`, `PEB:363-380`. Program `com.legend.equivalence.PeRatchets` (`PE/PeRatchets.java:31`).
2. **What it computes.** Two measured counts that tests used to pin by hand: `mutation.deck` (how many mutants the mutation
   fuzzer generates from the sibling-corpus fixtures, `PE/MutationFuzzTest.java:39-45`) and `own_corpus.matched` (how many
   elements of OUR test snippets give byte-identical protocol JSON from our `PmcdParser` and the engine's parser,
   `PE/OwnCorpusLedgerDraft.java:29-49`).
3. **Why it exists.** `7105b4e12` (2026-10-05, "Parser-equivalence's hand-copied measurements become a generated report",
   workplan P2-16 (b), decision D9). The committed file is what `MutationFuzzTest` and `OwnCorpusParityTest` compare their live
   value with (`PeRatchets.measured`).
4. **Inputs, declared** (aq: 17,916). Trees; `:srcs` (PE `src/**`, 320 files in the action); `//core:srcs`, `//pct:srcs`,
   `//spec:srcs` (as gen_roster); oracle pins; 435 jars.
5. **Inputs, actually read.** `ModuleFiles.in("parser-equivalence/src/test/resources/sibling-corpus/fixtures")` for the fixture
   names and each fixture as a classpath resource (`MutationFuzzTest.java:295-310`); `InlineSnippets.extract` over
   `core|spec|pct/src/test/**.java` (`PE/OwnCorpusConformanceTest.java:25-32`); the engine parser (classpath; `MutationFuzzTest`'s
   static `ORACLE`, `:82`; `ParserEquivalence`). **Our code it truly calls:** `PeRatchets`, `MutationFuzzTest` (a test class),
   `OwnCorpusLedgerDraft`, `OwnCorpusConformanceTest` (a test class), `ParserEquivalence`, `InlineSnippets`, `ModuleFiles`,
   `Corpus.Source`, `//testing`, and **our parser**: `com.legend.parser.PmcdParser.parseSections` (`PE/ParserEquivalence.java:76-78`,
   `//core:parser`). Over-declared: BOTH upstream trees and the oracle pins (no code path reads `Corpus.load`, `OraclePins` or a
   root: grep of the reached classes), core/pct/spec `src/main` + non-java files, PE `src/**` except the fixtures directory.
   Under-declared: none.
6. **Outputs.** `parser-equivalence/generated/ratchets.tsv`: a comment line, then `mutation.deck` and `own_corpus.matched`.
7. **Committed?** Yes: `parser-equivalence/src/test/resources/com/legend/equivalence/ratchets.tsv`; writer
   `//parser-equivalence:update_ratchets` (`PEB:382-388`, in `//:update_generated`); diff test `:update_ratchets_test`, suite
   `:update_ratchets_tests` in `//:generated` (`BUILD.bazel:91`).
8. **Who consumes it.** Gate 8 tests `MutationFuzzTest` and `OwnCorpusParityTest` via `PeRatchets.measured`, from the classpath
   copy in `pe_tests_lib` (`PeRatchets.java:46-83`). Humans: failure message `PEB:385`; `PeRatchets.java:53,64,80`.
9. **Determinism.** Two integer counts from sorted inputs (`MutationFuzzTest.java:299-301`, ModuleFiles' sorted walk). No clock.
10. **Cost.** Default heap; our parser and the engine's over >500 own snippets (floor at `OwnCorpusLedgerDraft.java:31-33`),
    plus mutant generation (text only). No DB.
11. **What reruns it today.** Every core edit (`somepath(//parser-equivalence:ratchets, //core:exec)` = `ratchets →
    pe_tests_lib → //core:core → //core:exec`); core/pct/spec `src/**`; PE `src/**` including the committed ratchets.tsv on its
    own classpath; trees (never read); @maven_upstream.
12. **What SHOULD rerun it.** Our parser (`//core:parser` and its deps base, error, lexer, model, protocol, spi, values, json);
    our test `.java` snippets in core/spec/pct; the sibling-corpus fixtures and mutation operators; an upstream bump (the oracle
    jars).
13. **Who runs it today.** `bazel build //...`; `//gates:local`/CI checks via `//:generated` (`local → //:generated →
    update_ratchets_tests → update_ratchets_test → ratchets`); the bump; by hand `bazel run //parser-equivalence:update_ratchets`.
14. **Recommendation: COMMITTED-SOURCE.** A measured ratchet whose real trigger is our parser and our snippets. `manual`: yes.
    Writer: explicit re-pin (`update_ratchets`), also run by the bump's update. Diff test: the everyday gate (it is where parser
    changes are checked; gate 8's tests repeat the comparison but run only in CI). Narrowing: a ratchets library holding the
    measured code (move `deckSize`/`ownSnippets` out of the two test classes) with deps `//core:parser`, `//core:diagnostics`,
    `//testing`, @maven_upstream grammar/protocol/shared_core + extension grammars; srcs = the three `:test_java` + the fixtures
    directory; drop UPSTREAM_TREES and the pins.
15. **Open questions.** None beyond the extraction of the two test-class methods.

## 5. //parser-equivalence:gen_own_corpus_draft

1. **Identity.** `java_run`, `PEB:392-413`. Program `com.legend.equivalence.OwnCorpusLedgerDraft`
   (`PE/OwnCorpusLedgerDraft.java:51`).
2. **What it computes.** A DRAFT of `docs/own-corpus-protocol-diffs.tsv`: every element of our own test snippets whose wire
   JSON from our parser differs from the engine's, each row keeping the reason from the committed ledger, new rows marked
   `TODO: adjudicate` (`:55-65`).
3. **Why it exists.** Introduced as a Bazel program in `998f1a41c`; the ledger is "upstream boundary batch 6". The reasons are
   human review, so it is deliberately not a generated file (`docs/BUILD.bazel:21-34`, `OwnCorpusLedgerDraft.java:10-16`).
4. **Inputs, declared** (aq: 17,597). Trees; `//core:srcs`, `//pct:srcs`, `//spec:srcs`; `//docs:own-corpus-protocol-diffs.tsv`;
   pins; 435 jars.
5. **Inputs, actually read.** Our snippets from core/spec/pct `src/test/**.java` (`ownSnippets`); the committed ledger (argument
   1, `OwnCorpusParityTest.readLedger(Path)`, `PE/OwnCorpusParityTest.java:91-102`). **Our code:** as §4 minus MutationFuzzTest,
   plus `OwnCorpusParityTest.readLedger`; **our parser** (`PmcdParser`). Over-declared: trees, pins, the `src/main` files.
6. **Outputs.** `parser-equivalence/generated/own-corpus-protocol-diffs.tsv`: header + `source#element \t divergence \t reason`.
7. **Committed?** Not as a generated file. `//docs:draft_own_corpus_ledger` (`docs/BUILD.bazel:27-34`, `diff_test = False`, not
   in `//:update_generated`) copies it over the hand-owned `docs/own-corpus-protocol-diffs.tsv` when a human runs it.
8. **Who consumes it.** A human finishing the ledger. The hand-owned ledger is gate 8's (`OwnCorpusParityTest`). Docs:
   `docs/GATES.md:62-63`; `OwnCorpusParityTest.java:37,80`; `OwnCorpusLedgerDraft.java:15`.
9. **Determinism.** TreeMaps (`:35-36`). Deterministic.
10. **Cost.** As §4's own-corpus half.
11. **What reruns it today.** As §4 (every core edit, `somepath` → `pe_tests_lib → //core:core → //core:exec`), plus the ledger.
12. **What SHOULD rerun it.** A human request.
13. **Who runs it today.** `bazel build //...` (neither it nor `//docs:draft_own_corpus_ledger` is manual), so CI's build lane
    and the bump's phase 3 (`bazel test //...` builds non-test targets too) compute and discard it. Not `//gates:local`
    (classpath edge only, F2). By hand: `bazel run //docs:draft_own_corpus_ledger` (GATES.md:63).
14. **Recommendation: DRAFT-MANUAL.** `manual`: yes (generator and writer; design §F, `…DESIGN…md:224-226,313`). Writer: neither
    update group. No diff test. Narrowing as §4 (srcs = the three `:test_java` + the ledger; no trees, no pins).
15. **Open questions.** None.

## 6–9. The four censuses (shared shape)

`[java_run(...) for name, (main, files) in _REPORTS.items()]`, `PEB:418-448`; `filegroup diagnostics_reports` `PEB:450-455`.
All four: `tags = ["manual"]`, `memory_mb = 2048`, `mnemonic = "Measure"`, `deps = [":pe_tests_lib"]`, srcs = `_INPUTS`
(`PEB:129-137`: core/pct/spec/PE test `.java`, the sibling corpus, the pins) + trees + `:engine_jar_files`/`:engine_jars_exec` +
`:gen_fixtures` + `:gen_roster`; flags pins, `-Dlegend.engine.fixtures`, `-Dlegend.engine.jars`, `-Dpe.roster`; arguments
`{OUT_DIR}`; each writes its report(s) plus `run.log` (stdout/stderr captured by `com.legend.testing.Programs.captureConsole`,
`testing/src/main/java/com/legend/testing/Programs.java:25-31`). aq: 16,856 inputs each; 435 jars (31 core). They became report
actions in `0ad319cfd` (2026-10-05, P3-17: "no test that only prints"). None asserts. Consumers: the manual filegroup and humans
(`docs/GATES.md:61`; `docs/IN_FLIGHT.md:328` "run only on their triggers (a pin bump, a parser/lexer/protocol change, a corpus
manifest change)"). Who runs them: nobody automatically: manual, not pulled in by any non-manual target (only the analysis-only
classpath edge, F2), no CI lane, not the bump. Output: `bazel-bin/parser-equivalence/<name>/`. Not committed.
Rerun today: any core edit (`somepath(//parser-equivalence:corpus_census, //core:exec)` = `corpus_census → pe_tests_lib →
//core:core → //core:exec`), any core/pct/spec/PE test `.java` edit (declared, never read), fixtures, roster, trees, jars.
Determinism: the reports use TreeMaps and stable count sorts; `run.log` holds whatever the code prints (OPEN whether any engine
logging there carries time; harmless for a report).

### 6. //parser-equivalence:corpus_census
- **Program** `com.legend.equivalence.CorpusCensus` (`PE/CorpusCensus.java:46`), origin `9f10efafb` (2026-08-08).
- **Computes** "the honest denominator": for every corpus source, BOTH-PARSE / OUR-DEFECT / REFERENCE-REFUSES (and whether we
  read what the engine refuses), with ranked error buckets (`:12-36,71-127`). Outputs `corpus-census.txt`,
  `corpus-census-defects.txt`, `run.log`.
- **Reads** `Corpus.all()` (trees, fixtures output, pins; `:49`), engine `PureGrammarParser` (`:54`), **our parser** via
  `Surfaces.engine/platform` (`ElementParser.parse`, `PE/Surfaces.java:18-59`; `//core:parser`, `:lexer`, `:model`).
  Over-declared: `_INPUTS`' test trees and sibling corpus, `:gen_roster`, the jars list.
- **True trigger:** a human question (parser work, a bump). **Recommendation: DRAFT-MANUAL** (a report). `manual` already;
  no writer; no diff test. Narrowing: deps `//core:parser` (+ its deps), `//core:diagnostics`, `//testing`, engine grammar jars;
  srcs trees + `:gen_fixtures` + pins only.

### 7. //parser-equivalence:grammar_keyword_census
- **Program** `com.legend.equivalence.GrammarKeywordCensus` (`PE/GrammarKeywordCensus.java:30`), origin `1ef3da335` (2026-08-12).
- **Computes** every word-shaped keyword literal in the engine's `.g4` files, and which ones appear in a corpus source that
  BOTH parsers accept; prints the uncovered keywords per grammar (`:38-115`). Outputs `grammar-keyword-census.txt`, `run.log`.
- **Reads** every `.g4` by walking the engine root (`:39-58`), `Corpus.all()` + `engineFixtures()` (`:64-66`), engine parser,
  **our parser** (`Surfaces.platform`, `:74`). Over-declared: test trees, sibling corpus, roster, jars list.
- **Recommendation: DRAFT-MANUAL.** Same narrowing as §6 (could take `@legend_engine_src//:grammars` instead of the tree for the
  `.g4` walk). Note: its question overlaps `//scripts/parser:keyword_coverage` (§13), which measures the same keywords against
  OUR `.pure` instead of upstream's corpus (`scripts/parser/keywords.py:1-24` explains the difference).

### 8. //parser-equivalence:migration_sizing
- **Program** `com.legend.equivalence.MigrationSizing` (`PE/MigrationSizing.java:54`), origin `4e61fa352` (2026-08-08).
- **Computes** for every corpus source with `###Mapping`/`###Relational`, whether the "legacy" path (`Surfaces.engine` =
  `ElementParser.parse(LEGEND_ENGINE)`) and the "protocol" path (`MappingProtocolParser`/`DatabaseProtocolParser` per token)
  agree (`:70-154`). Outputs `migration-sizing.txt`, `migration-legacy-only.txt`, `run.log`.
- **Reads** `Corpus.all()`, our `Lexer`/`TokenStream` and parsers. No engine class at all (Jackson only, via Corpus).
- **Why it exists, and that the reason is gone.** It sized the migration off `MappingGrammarParser` and
  `RelationalGrammarParser` (`:19-26`). Both were deleted the same day in `b23f68757` (2026-08-08, "M4: delete
  MappingGrammarParser and RelationalGrammarParser": "ElementParser routed ###Relational to protocol at R3 and ###Mapping in the
  previous commit"), and `ls core/src/main/java/com/legend/parser/` shows neither. Both of its paths now run the same protocol
  parsers. Its output is read by nothing.
- **Recommendation: DEAD.** Proof: the only references are `PEB:421` and its own file (`grep -rln "migration_sizing\|MigrationSizing"` outside `runs/` and `bazel-*` finds
  only those two plus a comment in `core/src/test/java/com/legend/SkipCensusTest.java:60` and `docs/PARSER_COMPLETENESS_PLAN.md:273`, both naming the
  deleted `MigrationSizingTest`). Delete the
  program and its `_REPORTS` row.

### 9. //parser-equivalence:pmcd_reachability_census
- **Program** `com.legend.equivalence.PmcdReachabilityCensus` (`PE/PmcdReachabilityCensus.java:36`), origin `1ef3da335`.
- **Computes** the protocol classes reachable from `PureModelContextData` (fields + Jackson subtype edges, BFS), then splits the
  roster's UNCOVERED tags into in-scope (a fixture worklist) and provably unreachable (`:44-163`). Outputs
  `pmcd-reachability-census.txt`, `run.log`.
- **Reads** the jars list (`:48-83`), the extension registry (`:84-95`), and `-Dpe.roster` = `:gen_roster`'s output
  (`:134-136`). **No lite code, no core class, no corpus, no tree, no pins.** Our code: the census and `//testing`.
  Over-declared: trees (15,595), `_INPUTS`, `:gen_fixtures`, pins, all 31 core jars.
- **True trigger:** an upstream bump (via the jars and the roster); a human question. **Recommendation: DRAFT-MANUAL.** Narrowing:
  deps `//testing` + the engine protocol jars + Jackson annotations (its own tiny library); srcs the jars list and `:gen_roster`.

## 10. //pct:adapter_par

1. **Identity.** `java_run`, `pct/BUILD.bazel:36-53`, `testonly`, `memory_mb = 4096`, mnemonic `PureParGenerator`. Program
   `com.legend.tools.par.ParGenerator` (`tools/par/ParGenerator.java:29`), library `//tools/par:par_generator`
   (`tools/par/BUILD.bazel:8-21`).
2. **What it computes.** Compiles our Pure PCT adapter (repository `core_legend_lite_pct`: `pct_adapter.pure`, `pct_native.pure`,
   `pct_types.pure` + its definition JSON) against the engine's already-compiled platform (`platform`, `platform_dsl_tds`, `core`,
   loaded from the PARs inside the classpath jars) and serializes it as `pure-core_legend_lite_pct.par`, the binary form the
   engine's interpreted runtime loads. Same call as legend-pure's Maven `build-pure-jar` goal: `PureJarGenerator.doGeneratePAR`
   (`ParGenerator.java:11-24,39-48`).
3. **Why it exists.** The PCT integration dates from `e16b035ce` (2026-01-28, Maven `build-pure-jar`); the Bazel program from
   `998f1a41c`; genrule → java_run in P1-22 (`BAZEL_EXECUTION_LOG.md:87-89`). The runtime "refuses to start without it"
   (`pct/BUILD.bazel:30-33`). Live.
4. **Inputs, declared** (aq: 213). `glob(["src/main/resources/**"])` = 4 files (`pct/BUILD.bazel:39`); root `{SRC}` = the
   definition's directory (`:51`); deps `//tools/par:par_generator`: 92 jars = 1 ours (`libpar_generator.jar`) + 90 @maven_upstream
   (`legend-pure-m3-core`, `legend-engine-pure-code-compiled-core`, `legend-pure-m2-dsl-tds-pure` and their closure) + jrt-fs;
   JDK. No upstream source archive, no core, no pins.
5. **Inputs, actually read.** The 4 files (`{SRC}` dir → `MutableFSCodeStorage` for the repository dir, the definition file as
   an "extra repository", `PURE/generator/par/PureJarSerializer.java:76-113`, `PureJarGenerator.java:80-98`); code repositories
   and their PARs from the classpath (`CodeRepositoryProviderHelper.findCodeRepositories`, `GraphLoader.findJars`); the
   platform version from `META-INF/maven/org.finos.legend.pure/legend-pure-m3-core/pom.properties` (`ParGenerator.java:54-64`).
   **Our code it calls:** `ParGenerator` only. Over/under-declared: none.
6. **Outputs.** `pct/pure-core_legend_lite_pct.par`: a jar with `META-INF/MANIFEST.MF`, `definition-index.json`,
   `reference-index.json` and one `.pc` binary per source (inspected with Python `zipfile`, 6 entries).
7. **Committed?** No. Build output, consumed as a classpath resource: `:adapter_par_jar` (`pct/BUILD.bazel:64-69`,
   `resource_strip_prefix` puts it at the classpath root) → `:adapter` (`:57-62`) → `:pct_tests_lib` runtime_deps (`:84`).
8. **Who consumes it.**
   - **Uses it:** the 11 Channel A tests: `pct_duckdb_{essential,grammar,relation,standard,unclassified}` (gate 6),
     `pct_h2` (gate 7), `pct_postgres_*` ×5 (gate 7P). They run legend-engine's PCT framework on the interpreted runtime, which
     finds the repository through `LegendLitePCTCodeRepositoryProvider` (`pct/src/test/java/org/finos/legend/pure/code/core/
     LegendLitePCTCodeRepositoryProvider.java:30`, registered in `pct/src/test/resources/META-INF/services/…CodeRepositoryProvider`)
     and loads its PAR.
   - **Carries it without using it:** the 5 Channel B tests `pct_channel_b_*` (gate 9) and `//pct:ratchets`, because they share
     `pct_tests_lib` (`somepath(//pct:pct_channel_b_essential, //pct:adapter_par)` = `… → pct_tests_lib → adapter →
     adapter_par_jar → adapter_par`; same for `//pct:ratchets`). **Channel B does not need it:** `ChannelB.java` imports only
     `com.legend.*` and `java.*` (grep of `CHB/*.java` for non-`com.legend`/`java`/`org.junit` imports finds none); it parses and
     compiles the PCT sources with OUR `Compiler.parseSources`/`Compiler.buildModel` and executes on in-memory DuckDB
     (`CHB/ChannelB.java:102-167,262`). No engine runtime, no PAR.
   - Also `//:update_generated` (through `//pct:ratchets`) and the analysis-only guards (F2). Query:
     `kind(".*_test", rdeps(//..., //pct:adapter_par))` = the 16 PCT tests + `//pct:update_ratchets_test` +
     `//tools/guards:{classpath,markdown_inputs}_test`.
9. **Determinism: NOT byte-reproducible, and the cause is found.** Every zip entry carries the wall-clock time of the run.
   legend-pure's `PureRepositoryJarBuilder` writes entries with `new JarEntry(name)` and never sets a time
   (`…/serialization/runtime/binary/PureRepositoryJarBuilder.java:44,59,102,129`), and the JDK's `ZipOutputStream.putNextEntry`
   stamps an entry whose time is unset with `System.currentTimeMillis()` (the manifest entry written by the `JarOutputStream`
   constructor likewise). **Evidence:** three PARs built on 2026-09-27, 2026-10-03 and 2026-10-04 (`bazel-bin` of the main
   checkout and of worktrees under `runs/`) have different file SHA-256s (`d8a1e588…`, `0b5b76ed…`, `d64a0338…`) but IDENTICAL
   CRC-32s for all six entries (`MANIFEST.MF:be559c78 definition-index.json:d4855bb4 reference-index.json:32798e34
   pct_adapter.pc:525a6d85 pct_native.pc:008fc682 pct_types.pc:4fde24d1`); each file has one distinct entry timestamp, the time
   it was built. The content is reproducible; only the DOS timestamps differ. (The plan's "anonymous ids … and the order of the
   lines" record, `BAZEL_FIRST_CLASS_WORKPLAN_2026_10_03.md:1396`, describes the reference dump's text; for this PAR no content
   variance shows. legend-pure's compiler does key some collections by `CoreInstance` identity hash,
   `legend-pure-m4/…/AbstractCoreInstance.java:34-36`, so a larger repository could also vary in content; OPEN, see 15.)
   Effect: Bazel caches the action by inputs, so a rebuild happens only on an input change or a cache miss (a fresh machine, a
   CI cache key change, eviction); each such rebuild gives new bytes, so all 17 dependants (11 Channel A tests, 5 Channel B
   tests, `//pct:ratchets`) rerun and miss any shared cache.
   **Fix:** in `ParGenerator`, after `doGeneratePAR`, rewrite the jar with every entry's time set to a constant (e.g. 1980-01-01
   or `SOURCE_DATE_EPOCH`) and entries in their current order; then add a byte diff of two runs as a check.
10. **Cost.** 4096 MB heap and resource_set (live peak 2,099 MB, `pct/BUILD.bazel:48`); loads the compiled platform from PARs
    and compiles three files. One JVM, no DB.
11. **What reruns it today.** `pct/src/main/resources/**`, `tools/par/ParGenerator.java`, the 90 @maven_upstream jars, the JDK.
    No core (`somepath(//pct:adapter_par, //core:exec)` is empty).
12. **What SHOULD rerun it.** Exactly that: the adapter's Pure, the PAR generator, an upstream bump (the jars).
13. **Who runs it today.** `bazel build //...` (not manual) and CI lanes 6, 7, 7p, 9 (`gates-run.yml:55-59`) through their tests;
    the bump's phase 2 (through `//pct:ratchets`' classpath) and phase 3. Not `//gates:local` (only the analysis edge, F2;
    `//pct:pct_discipline` has its own library, `pct/BUILD.bazel:133-155`). No doc tells a human to run it.
14. **Recommendation: BUILD-OUTPUT** (testonly: it already is). `manual`: no. No writer, no diff test (add a two-run byte check
    once reproducible). Narrowing is already right for the generator; the fix is downstream: split `pct_tests_lib` so Channel B
    and the ratchets generator get a library without `:adapter` and without the @maven_upstream PCT jars (design R5, `…DESIGN…md:123`).
15. **Open questions.** (a) Is the CONTENT reproducible run to run on one machine, beyond the three cached samples above (which
    may share inputs but were produced by separate actions)? Settle: build twice with `--disk_cache=` empty and
    `--noremote_accept_cached`, compare entry CRCs. (b) Does Channel B need `//core:shadow_binding` or anything else
    `pct_tests_lib`'s runtime_deps give it? Settle by running gate 9 on a split library.

## 11. //pct:ratchets

1. **Identity.** `java_run`, `pct/BUILD.bazel:256-272`, `manual`, `memory_mb = 2048`. Program
   `org.finos.legend.lite.pct.channelb.PctRatchets` (`CHB/PctRatchets.java:32`).
2. **What it computes.** Each Channel B suite's DISCOVERY count: how many `PCT.test` functions our own compiler finds in the
   pinned PCT sources, per suite (essential, grammar, relation, standard, unclassified). It gets them by running each suite in
   full (`ChannelB*Test.runSuite`), which parses and compiles the platform and executes every discovered test on in-memory DuckDB,
   then keeps only the count (`PctRatchets.java:36-45`).
3. **Why it exists.** `4c24d7db2` (2026-10-05, P2-16 (b), pct). The Channel B tests compare their live discovery with this file
   (`CHB/ChannelBEssentialTest.java:66` and the four siblings), replacing hand-copied constants.
4. **Inputs, declared** (aq: 15,890). UPSTREAM_TREES; deps `:pct_tests_lib` + `//core:drivers`: 180 jars = 39 ours (33 core incl.
   `duckdb_load`/`shadow_binding`, base, json, testing, `pct_tests_lib`, `adapter`, `adapter_par_jar`) + 140 external + jrt-fs.
5. **Inputs, actually read.** The pure tree (all five suites' model root `legend-pure-m3-core/src/main/resources/platform/pure`)
   and the engine tree (relation, standard, unclassified scopes) via `ProgramPaths.rootOf` (`CHB/ChannelB*Test.java:25-40` (`runSuite` at `:30-35` in each)),
   walked in fixed order (`CHB/ChannelB.java:102-112`, `SourceWalk.inOrder`); DuckDB in process (`ChannelB.java:262`);
   `-Dlegend.diagnostics` (`chb-only`). **Our code it truly calls:** `Compiler` (planner), `Execution`/`ExecuteOptions` (driver),
   `NameResolver`, `ModelContext`, `exec.CanonicalDivergence`/`SqlTypeCensus`, `lowering.StampCensus`, `probe.Shadow`,
   `test.StorelessRuntime`, `parser.Dialect`, model/protocol/error/diagnostics/platform classes, `//testing` (grep of `CHB/`).
   So it genuinely needs `//core:exec`. Over-declared: the PAR and `:adapter`, every @maven_upstream PCT/engine jar
   (Channel B imports none). Under-declared: none.
6. **Outputs.** `pct/generated/ratchets.tsv`: a comment line and five `channel_b.<suite>.discovered` counts.
7. **Committed?** Yes: `pct/src/test/resources/org/finos/legend/lite/pct/channelb/ratchets.tsv`; writer `//pct:update_ratchets`
   (`pct/BUILD.bazel:274-282`, manual, in `//:update_generated`); diff test `//pct:update_ratchets_test` is manual and in no suite
   that runs (`//:generated` omits `//pct:update_ratchets_tests`, `BUILD.bazel:79-97`). The effective check is gate 9.
8. **Who consumes it.** The five Channel B tests (gate 9) via `PctRatchets.measured` from the classpath copy in `pct_tests_lib`
   (`PctRatchets.java:50-86`). Humans: `pct/BUILD.bazel:277`, `PctRatchets.java:57,68,83`, each Channel B assertion message.
9. **Determinism.** Integer counts; inputs walked in fixed order. Deterministic.
10. **Cost.** 2048 MB; compiles the platform five times and executes every Channel B test (DuckDB in process). The heaviest
    "generator" here, though only a count is kept.
11. **What reruns it today.** Any core edit (`somepath(//pct:ratchets, //core:exec)` = `ratchets → pct_tests_lib → //core:core →
    //core:exec`); any pct test source or resource (including its own committed copy on the classpath); the PAR
    (`somepath(//pct:ratchets, //pct:adapter_par)` = `ratchets → pct_tests_lib → adapter → adapter_par_jar → adapter_par`), so
    every non-reproducible PAR rebuild; @maven_upstream; trees.
12. **What SHOULD rerun it.** An upstream bump (new PCT functions: "327 -> 345 at the 4.145.0 bump",
    `ChannelBEssentialTest.java:63-65`); also our parser/compiler, when a parse or model wall moves (walls drop source files and
    their tests, `ChannelBEssentialTest.java:45-53`; `ChannelB.java:128-167`).
13. **Who runs it today.** Despite `manual`: `bazel build //...` (through the non-manual `//:update_generated`, query above; design
    R4, `…DESIGN…md:114-117`), so CI's build lane; the bump's phase 2. Not `//gates:local` (F2). By hand
    `bazel run //pct:update_ratchets` (messages above).
14. **Recommendation: COMMITTED-UPSTREAM** (with a secondary parser/compiler trigger that gate 9 catches). `manual`: yes, and
    leave the non-manual root writer. Writer: the bump-only update plus an explicit re-pin. Diff test: none needed beyond gate 9
    (or its own lane). Narrowing: a Channel B library (`CHB/*.java`) with deps `//core:planner`, `//core:driver`, `//core:test`,
    `//core:compiler`, `//core:exec`, `//core:lowering`, `//core:probe`, `//core:model`, `//core:protocol`, `//core:parser`,
    `//core:error`, `//core:platform`, `//core:diagnostics`, `//base`, `//testing`, runtime `//core:drivers`; no `:adapter`, no
    PAR, no @maven_upstream. Optionally stop after discovery instead of executing every test.
15. **Open questions.** Can discovery be counted without execution (only `isPctTest` over the compiled model,
    `ChannelB.java:195`)? Settle by reading the rest of `ChannelB.run`.

## 12. //tools/engine-runner:vocab

1. **Identity.** `java_run`, `tools/engine-runner/BUILD.bazel:60-69`, testonly. Program `perf.TokenDump`
   (`tools/engine-runner/src/main/java/perf/TokenDump.java:39`), in `:runner` (`BUILD.bazel:9-45`).
2. **What it computes.** The token vocabulary the runner's jars can actually lex: for every ANTLR-generated lexer class on the
   classpath, its `VOCABULARY` literal names (length ≥ 3, quotes stripped), one line per lexer simple name (`TokenDump.java:14-36`).
3. **Why it exists.** `cc164361c` (2026-08-13): the keyword census had harvested `.g4` from a working copy at HEAD while the
   runner used released jars, so five keywords counted as missing did not exist in the jars (`TokenDump.java:17-26`). Bazel target
   in `d2c0dd6a6` (2026-10-04, P2-18). Since both now come from ONE pinned release (`release.MODULE.bazel:1-21`), that skew
   cannot recur through this route.
4. **Inputs, declared** (aq: 771). No srcs; deps `:runner`: 654 jars = 34 ours (31 core, base, json, runner) + 619 @maven_runner
   + jrt-fs; JDK.
5. **Inputs, actually read.** Every jar on `java.class.path`, every class whose simple name ends in `lexer`/`LexerGrammar`,
   initialized to read `VOCABULARY` (`TokenDump.java:43-67,88-136`). Our jars are opened too; core's only `*Lexer` class,
   `com.legend.lexer.Lexer`, has no `VOCABULARY` field (grep), so ours contribute nothing. **Our code it calls:** `TokenDump`
   only; it never touches `//core` (only `LiteParseMain` in `:runner` does). Over-declared: all 31 core jars, base, json, and the
   non-grammar @maven_runner jars (execution stack, H2, DuckDB).
6. **Outputs.** `tools/engine-runner/generated/vocab.tsv`: `<Lexer> \t lit \t lit …` per lexer (91 lines committed).
7. **Committed?** Yes: `tools/engine-runner/vocab.tsv`; writer `//tools/engine-runner:update_vocab` (`BUILD.bazel:71-77`, in
   `//:update_generated`); diff test `:update_vocab_test` in `:update_vocab_tests` in `//:generated` (`BUILD.bazel:94`).
8. **Who consumes it.** **No build target and no test.** `grep -rn vocab.tsv` finds `TokenDump.java`, the BUILD, and
   `scripts/parser/fixtures.py`, which has no Bazel target (BUILD comment: "The other scripts here are history (P7-04)",
   `scripts/parser/BUILD.bazel:1-3`). `keywords.py` defines `runner_vocabulary`/`version_skew` (`scripts/parser/keywords.py:116-162`)
   but its `main()` calls neither (`:372-442`); only `fixtures.py:276-277` does. `bazel run //scripts/parser:keywords` passes
   `--vocab` (`scripts/parser/BUILD.bazel:41-42`) and never reads it. And `//scripts/parser:keyword_coverage` carries it only as
   its tool's runfiles (§13). Humans: `scripts/parser/HANDOFF.md:85-90` ("after a release bump … update_vocab"),
   `scripts/parser/README.md:130-133`, `TokenDump.java:31-33`.
9. **Determinism.** TreeMap/TreeSet; lexers that fail to initialize are skipped per class (`:113-127`), deterministic for a fixed
   classpath order. Diff-tested on three platforms.
10. **Cost.** Default heap; opens 654 jars, initializes every lexer class. **Built twice**: aq shows a second `Generate
    //tools/engine-runner:vocab [for tool]` in `darwin_arm64-opt-exec`, with `Building core/libcore.jar … [for tool]`,
    `libexec.jar … [for tool]` etc. (aquery `mnemonic("Generate|Javac", deps(//scripts/parser:keyword_coverage))`): all of core
    compiled a second time in the exec configuration, which P1-22 set out to remove
    (`BAZEL_FIRST_CLASS_WORKPLAN_2026_10_03.md:1118`).
11. **What reruns it today.** Any core edit (`somepath(//tools/engine-runner:vocab, //core:exec)` = `vocab → runner → //core:core
    → //core:exec`), in both configurations; any `perf/*.java`; @maven_runner.
12. **What SHOULD rerun it.** An upstream bump (@maven_runner is repinned by the bump, `Bump.java:136-142`).
13. **Who runs it today.** `bazel build //...`; `//gates:local`/CI checks via `//:generated` (`local → //:generated →
    update_vocab_tests → update_vocab_test → vocab`); the exec copy through `keyword_coverage` (same suite); the bump; by hand per
    HANDOFF.md.
14. **Recommendation: DEAD** as a committed file: nothing reads it (proof in 8). If the user wants the vocabulary as a legible
    bump record (as with the manifest), it is COMMITTED-UPSTREAM instead: `manual`, bump-only writer and check. Either way:
    move `TokenDump` into its own library with deps `@maven_runner//:org_antlr_antlr4_runtime` + the runner's grammar jars only
    (no `//core`), and remove it from `keywords`' data (§13). If kept, decide whether `fixtures.py` lives (P7-04).
15. **Open questions.** Does the user want `fixtures.py` (the only reader) revived as a target, or retired with P7-04?

## 13. //scripts/parser:keyword_coverage

1. **Identity.** `run_binary`, `scripts/parser/BUILD.bazel:73-94`; tool `:keywords` (`py_binary`, `:34-50`, testonly), script
   `scripts/parser/keywords.py` (`main` at `:372`), with `tiers.py`.
2. **What it computes.** Every typeable keyword literal in the pinned engine's `.g4` lexer grammars, grouped by grammar, and how
   many of them appear as words in the `.pure` this repository owns (comments and strings stripped); one TSV row per in-scope
   grammar plus tier totals (`keywords.py:85-113,292-309,352-369`). It refuses an unclassified grammar (`:380-383`).
3. **Why it exists.** `173845206` (2026-10-04, P2-20, "The keyword census is a Bazel target over pinned inputs; no host checkout");
   the census itself is from the parser-completeness work (`cc164361c`). A golden of how much of the engine's keyword surface
   our own Pure exercises (D9).
4. **Inputs, declared** (aq: 5 action inputs): `:engine_grammars` (copy of `@legend_engine_src//:grammars`, `:54-59`), `:our_pure`
   (`:pure_sources` + `//core:test_pure` + `//scripts/corpus:pure_sources`, `:61-70`), `keywords.py`, and the tool `keywords` with
   its runfiles tree in `darwin_arm64-opt-exec`. Those runfiles hold `_DATA` (`:24-31`): the same `.pure` again,
   `@legend_engine_src//:grammars` and `pom.xml`, and **`//tools/engine-runner:vocab`**, which drags `:runner` and all of core into
   the exec configuration (`bazel cquery 'somepath(//scripts/parser:keyword_coverage, //core:exec)'` =
   `keyword_coverage (2e61a73) → keywords (2b4c0cb) → vocab (2b4c0cb) → runner (2b4c0cb) → //core:core (2925eff) → //core:exec`).
   Env `PYTHONHASHSEED=0`, `PYTHONUTF8=1` (`:89-92`).
5. **Inputs, actually read.** With `--out`: `--engine-root` (rglob `*.g4`) and `--ours` (rglob `*.pure`) only
   (`keywords.py:46-48,88-90,302-309,379-385`); it returns before any vocabulary code. **Our code:** `keywords.py`, `tiers.py`; no
   Java. Over-declared: vocab (and through it core, the runner, @maven_runner, in exec config), the tool's duplicate `.pure` and
   grammar runfiles, `pom.xml`. Under-declared: none.
6. **Outputs.** `scripts/parser/generated/keyword-coverage.tsv`: header, `tier \t grammar \t covered \t total \t missing` rows,
   then TOTAL rows (74 lines committed).
7. **Committed?** Yes: `scripts/parser/keyword-coverage.tsv`; writer `//scripts/parser:update_keyword_coverage` (`:96-102`, in
   `//:update_generated`); diff test `:update_keyword_coverage_test` in `:update_keyword_coverage_tests` in `//:generated`
   (`BUILD.bazel:92`).
8. **Who consumes it.** No program reads it (grep: only the BUILD, `keywords.py`, and a comment in
   `PE/GrammarKeywordCensus.java:18`). Its diff test is the consumer: a moved count fails `//:generated` ("read the diff",
   `:99`). Humans: `scripts/parser/HANDOFF.md:27,89`.
9. **Determinism.** `PYTHONHASHSEED=0`, every set sorted on output (`:358-368`), `newline="\n"`. Diff-tested on three platforms.
10. **Cost.** The script is cheap (regex over ~160 grammars and our `.pure`). Its tool's runfiles cost an exec-configuration
    compile of all of core and an exec run of vocab (§12.10).
11. **What reruns it today.** Engine grammars; any of our `.pure` in the three filegroups; `keywords.py`/`tiers.py`; and, through
    the tool runfiles, any change to vocab's exec output (core edits recompile core in exec config and rerun vocab, but vocab's
    bytes do not change, so this action itself is cut off; the cost is paid upstream of it).
12. **What SHOULD rerun it.** An upstream bump (grammars); an edit to our `.pure` (`scripts/parser/**`, `core/src/test/resources/**`,
    `scripts/corpus/**`); `keywords.py`/`tiers.py`.
13. **Who runs it today.** `bazel build //...`; `//gates:local`/CI checks via `//:generated` (`local → //:generated →
    update_keyword_coverage_tests → update_keyword_coverage_test → keyword_coverage`); the bump; by hand
    `bazel run //scripts/parser:update_keyword_coverage` (HANDOFF.md:89, `keywords.py:356`).
14. **Recommendation: COMMITTED-SOURCE (mixed: our `.pure` daily, the grammars on a bump).** `manual`: yes (generator); writer in
    `//:update_generated` (the design's group C/D everyday writer) and also run by the bump; diff test in the everyday gate (it
    checks our corpus files, cheap once narrowed). Narrowing: give `keyword_coverage` a tool with no data (a second `py_binary`,
    or `:keywords` without `_DATA`): its inputs are already passed as `srcs`. That alone removes the exec-config core compile from
    `//gates:local`.
15. **Open questions.** None.

## 14. //tools/reference:ref_dump

1. **Identity.** `java_run`, `tools/reference/BUILD.bazel:64-84`, testonly, manual, `memory_mb = 8192`, `-Xss16m`, mnemonic
   `ReferenceDump`. Program `RefResolutions` (default package; `tools/reference/RefResolutions.java:34`), library
   `:ref_resolutions` (`:51-58`).
2. **What it computes.** Compiles every Pure repository on the classpath with legend-pure's own interpreted runtime, then for every
   function-call expression in every concrete function whose source id starts with `/core_relational/`, `/platform/`,
   `/core_functions_` or `/core/`, prints source, line, column, spelling, the resolved function's FQN and id, and the enclosing
   function (`RefResolutions.java:23-114`; arguments `BUILD:68-74`). One row per call, nested calls and lambda bodies included.
3. **Why it exists.** `06eeb8142` (2026-09-29, "W1.1 (1): the reference lane, calls first"): the reference side of the reference
   lane, which compares our name resolution with legend-pure's over core_relational's closure (`tools/reference/README.md`).
4. **Inputs, declared** (aq: 229). deps `:ref_resolutions` → `_REFERENCE_JARS` (`:11-49`): 112 jars = 1 ours + 110 @maven_upstream
   (the compiler and the 27 modules' `-pure`/grammar jars) + jrt-fs; JDK. No archive, no core, no pins.
5. **Inputs, actually read.** The classpath's code repositories (`CodeRepositoryProviderHelper.findCodeRepositories()`,
   `ClassLoaderCodeStorage`, `:38-41`). **Our code:** `RefResolutions` only. Over/under-declared: none.
6. **Outputs.** `tools/reference/ref-resolutions.tsv` (TSV with header; a sampled build had 235,669 lines and 909 rows carrying
   anonymous ids `@_…`).
7. **Committed?** No. Build output consumed by `//spec:reference_lane_report` (`spec/BUILD.bazel:235-258`, manual), whose report is
   diff-tested against a committed golden by `//spec:update_reference_lane_test` (manual) and checked by `//spec:reference_lane`
   (manual test).
8. **Who consumes it.** `//spec:reference_lane_report` → `ReferenceJoin` (`spec/src/test/java/com/legend/generators/
   ReferenceLaneReport.java:52-61`), which writes a sorted, position-free report (`:72`). Humans: `tools/reference/README.md:5`,
   `docs/GATES.md:6198`; `tools/reference/join.py` and `source_drift.py` (scripts with no target) take it by argument.
9. **Determinism.** Not byte-reproducible, recorded: two runs differ in anonymous ids and line order
   (`BAZEL_FIRST_CLASS_WORKPLAN_2026_10_03.md:1396`; BUILD comment `:60-63`). In the code: it iterates `pkg._children()` in the
   runtime's order (`RefResolutions.java:47-48`, noted in `docs/EXECUTION_PLAN_2026_09_26.md:532-533`) and prints
   `getUserPathForPackageableElement` of resolved functions, which for column/lambda functions is a `@_…` anonymous name from a
   sequential counter (`legend-pure-m4/…/ModelRepository.java:897-915`); legend-pure keys some collections by `CoreInstance`
   identity hash (`AbstractCoreInstance.java:34-36`), so creation order, and so the ids, can vary. The consumer's report is sorted
   and has been run 10 times byte-identical (`EXECUTION_PLAN_2026_09_26.md:532-533`), so the variance does not reach the golden.
10. **Cost.** 8192 MB (live peak 3,072 MB, `:78`); ~35 s (`:1-4,62`); compiles the 27-module closure. One JVM.
11. **What reruns it today.** `RefResolutions.java`, the @maven_upstream jars, the JDK. No core (`somepath` empty).
12. **What SHOULD rerun it.** An upstream bump; an edit to `RefResolutions.java`.
13. **Who runs it today.** Only a human building the manual reference lane (`bazel test //spec:reference_lane` /
    `bazel build //spec:reference_lane_report`; GATES.md:6198, "every front-end slice's GATES entry cites a run",
    `spec/BUILD.bazel:229-232`). Not `//...` (manual, and its only rdeps are manual), no CI lane, not `//gates:local` (F2), not the bump.
14. **Recommendation: BUILD-OUTPUT** (testonly: yes). `manual`: yes, as now. No writer, no diff test of its own. Narrowing: already
    right (design: "already right", `…DESIGN…md:181`). Optional: make the output canonical (sort; drop or renumber anonymous ids)
    so its cache key stops changing downstream.
15. **Open questions.** Does `ReferenceJoin` read the `resolvedId` column for anonymous functions (would canonicalizing ids change
    the report)? Settle by reading `ReferenceJoin`.

## 15. //tools/reference:ref_imports

1. **Identity.** `java_run`, `tools/reference/BUILD.bazel:98-109`, testonly, manual, `memory_mb = 8192`, `-Xss16m`, mnemonic
   `ReferenceImports`. Program `RefImports` (`tools/reference/RefImports.java:21`), library `:ref_imports_lib` (`:87-94`, its own so
   editing it never reruns ref_dump).
2. **What it computes.** For every source, the union of the packages that the import group of each function body's FIRST call
   expression makes visible (implicit imports included), as the reference compiler sees it (`RefImports.java:18-45`).
3. **Why it exists.** `6743f0cdd` (2026-09-25, "the implicit imports measured"); report action in `0ad319cfd` (P3-17). It informs
   our implicit-import rules (`tools/reference/README.md:92-101`).
4. **Inputs, declared** (aq: 229): as ref_dump (`_REFERENCE_JARS`, 112 jars, JDK).
5. **Inputs, actually read.** Classpath repositories (`:22-25`). **Our code:** `RefImports` only. Over/under: none.
6. **Outputs.** `tools/reference/ref-imports.tsv`: `sourceId \t pkg,pkg,…` sorted.
7. **Committed?** No; nothing consumes it in the build (`grep ref-imports` finds only README and BUILD).
8. **Who consumes it.** Humans: `tools/reference/README.md:94-95` ("`bazel build //tools/reference:ref_imports`, its report in
   `bazel-bin/tools/reference/ref-imports.tsv`").
9. **Determinism.** TreeMap of TreeSets of package paths (`:26,35`); packages carry no anonymous ids. Likely deterministic; OPEN.
10. **Cost.** 8192 MB declared (unmeasured for this one); compiles the same closure as ref_dump.
11. **What reruns it today.** `RefImports.java`, @maven_upstream, JDK.
12. **What SHOULD rerun it.** A human request (after a bump or an import-rule question).
13. **Who runs it today.** Only by hand (README). Manual, no rdeps, no lane, not the bump.
14. **Recommendation: DRAFT-MANUAL** (a report). `manual`: yes, as now. No writer, no diff test. Narrowing: already right.
15. **Open questions.** Is it byte-stable (two runs)? Settle with two cold builds; matters only if someone compares runs.

## 16. //tools/java_run:pins

1. **Identity.** `java_run` (rule `_java_run`), `tools/java_run/BUILD.bazel:17-23`, mnemonic default `JavaRun`, NOT testonly.
   Program `com.legend.tools.javarun.pins.PrintPins` (`tools/java_run/pins/PrintPins.java:16`), library `:print_pins` (`:10-15`).
2. **What it is: a guard's subject, not a generator of anything used.** It prints what a java_run action sees: whether each of the
   four pinned flags is on the command line ("given"/"MISSING", read from `RuntimeMXBean.getInputArguments()`, so a host whose
   defaults happen to match cannot hide a removed pin), the effective timezone, locale and encoding, and the temp directory's name
   (`PrintPins.java:16-30`). `//tools/java_run:pins_test` (`diff_test`, `BUILD.bazel:25-29`) compares it with the hand-written
   `tools/java_run/pins/pins.expected`.
3. **Why it exists.** `cdbcbb698` (2026-10-03, "P0-10's proof: removing any pin fails a test (review of #17)"). It protects
   `tools/java_run/defs.bzl:84-91`: every generator's output must not depend on the machine's clock, locale or encoding.
4. **Inputs, declared** (aq: 119): `libprint_pins.jar` + the JDK (117 files incl. jrt-fs). No srcs.
5. **Inputs, actually read.** JVM input arguments, default TimeZone/Locale/Charset, `java.io.tmpdir`. No file. Over/under: none.
6. **Outputs.** `tools/java_run/pins.txt`: eight lines (four flags "given", `timezone=GMT`, `locale=en_US`, `encoding=UTF-8`,
   `tmpdir=pins_tmp`).
7. **Committed?** No. The expected file is hand-owned (no writer; `diff_test`, not `write_source_files`).
8. **Who consumes it.** `//tools/java_run:pins_test`, in `//gates:local` (`gates/BUILD.bazel:29`) and CI checks
   (`gates-run.yml:51`; `docs/GATES.md:32`).
9. **Determinism.** Yes; its job is to prove determinism of the environment. The last line depends on the target's name.
10. **Cost.** Trivial: one tiny JVM.
11. **What reruns it today.** `PrintPins.java`, `tools/java_run/defs.bzl` (it changes the action's command line), the JDK.
12. **What SHOULD rerun it.** The same. Already right.
13. **Who runs it today.** `bazel build //...`; `//gates:local` directly; CI checks; the bump's phase 3 (`bazel test //...`). No doc
    tells a human to run it.
14. **Recommendation: BUILD-OUTPUT**, consumed by `pins_test`. Mark `:pins` and `:print_pins` `testonly = True`. `manual`: no. No
    writer. Its check stays in the everyday gate (it guards the rule every generator uses). Narrowing: none needed.
15. **Open questions.** None.

---

## Cross-cutting findings

1. **The committed generated files are inputs of their own generators.** `pe_tests_lib` takes `glob(["src/test/resources/**"])`
   as resources (`PEB:51`), which holds `corpus-manifest.tsv`, `engine-grammar-fixtures.jsonl` and `ratchets.tsv`; `pct_tests_lib`
   likewise holds the pct `ratchets.tsv` (`pct/BUILD.bazel:81`). So `bazel run //:update_generated` changing any one of them
   reruns every PE generator (and `//pct:ratchets`) on the next build. Outputs do not depend on the committed copies (generators
   read the new harvest via `-Dlegend.engine.fixtures`), so this is wasted work, not a fixed-point problem.
2. **`pe_tests_lib` needs six core libraries, not 31.** Every `com.legend.*` reference in `parser-equivalence/src/test/java`
   outside the package is base, diagnostics, json, lexer, model, parser, protocol or testing (grep tally: 11/1/1/20/23/45/4/57).
   The generators split further: gen_fixtures and pmcd_reachability need no core; gen_manifest and gen_roster need only
   `:diagnostics`; ratchets, own draft and the three corpus censuses need `:parser` (+ `:lexer`, `:model`, `:protocol`).
3. **Channel B, `//pct:ratchets` and the PAR.** Channel B and the ratchets generator carry the PAR and the PCT engine jars without
   using either (§10.8). With the PAR's wall-clock timestamps (§10.9), every PAR rebuild invalidates gate 9 and the ratchets too.
4. **An exec-configuration copy of core in the light gate.** `//scripts/parser:keyword_coverage`'s tool data pulls
   `//tools/engine-runner:vocab`, so `//gates:local` (via `//:generated`) compiles all of core in `opt-exec` and runs vocab twice
   (§12.10, §13.4), for an input the script never reads.
5. **Declared but never read.** `docs/protocol-roster.tsv` and `corpus-manifest.tsv` in gate 8's data (§2.8, §3.8); UPSTREAM_TREES
   and the pins in `:ratchets` and `:gen_own_corpus_draft`; `core/src/main` via the three `:srcs` filegroups in gen_roster,
   ratchets and the draft; the test trees, roster and jars list in three of four censuses; the pure tree in gen_fixtures.

## Who the bump drives, and which docs send a human

- **Driven by `tools/bump` through `//:update_generated` (phase 2):** gen_fixtures, gen_manifest (via
  `//parser-equivalence:update_generated`), gen_roster (via `//docs:update_generated`), `//parser-equivalence:ratchets`,
  `//pct:ratchets` (and so the PAR), vocab, keyword_coverage. **Through `bazel test //...` (phase 3):** every non-manual target:
  the above again for their diff tests, the PAR (PCT tests), `//tools/java_run:pins`, and gen_own_corpus_draft (computed and
  discarded). **Not driven:** the four censuses, ref_dump, ref_imports. The judgement message names corpus-manifest.tsv,
  protocol-roster.tsv and the fixture snapshot (`Bump.java:160-166`).
- **Docs that tell a human to run one:** `bazel run //:update_generated`: `README.md:294`, `docs/GATES.md:47`, `PE/Corpus.java:201`,
  `RosterGenerator.java:43`, the diff messages at `PEB:352` and `docs/BUILD.bazel:14`. `update_vocab` and
  `update_keyword_coverage` "after a release bump": `scripts/parser/HANDOFF.md:85-90`, `scripts/parser/README.md:130-133`,
  `TokenDump.java:31-33`, `keywords.py:356`. `//parser-equivalence:update_ratchets`: `PEB:385`, `PeRatchets.java:19,39,53,64,80`.
  `//pct:update_ratchets`: `pct/BUILD.bazel:277`, `PctRatchets.java:42,57,68,83`, the Channel B assertion messages.
  `//docs:draft_own_corpus_ledger`: `docs/GATES.md:63`, `OwnCorpusParityTest.java:37,80`. `diagnostics_reports`:
  `docs/GATES.md:61`, `docs/IN_FLIGHT.md:328`, `PEB:415-417`. `ref_dump`/`ref_imports`: `tools/reference/README.md:5,94`,
  `docs/GATES.md:6198`. None for `adapter_par` or `java_run:pins`.

## Summary table

| label | recommendation | manual? | update group | true trigger |
|---|---|---|---|---|
| //parser-equivalence:gen_fixtures | COMMITTED-UPSTREAM | yes | bump-only | upstream release (+ harvest code) |
| //parser-equivalence:gen_manifest | COMMITTED-UPSTREAM (record only; DEAD if not wanted) | yes | bump-only | upstream release (+ Corpus/InlineSnippets) |
| //parser-equivalence:gen_roster | COMMITTED-SOURCE (mixed) | yes | bump update; diff test everyday once narrowed | upstream release; core/spec/pct test `.java` |
| //parser-equivalence:ratchets | COMMITTED-SOURCE | yes | explicit re-pin (+ bump) | `//core:parser`; own test snippets; PE fixtures; upstream jars |
| //parser-equivalence:gen_own_corpus_draft | DRAFT-MANUAL | yes | neither | human request |
| //parser-equivalence:corpus_census | DRAFT-MANUAL | yes (is) | neither | human request |
| //parser-equivalence:grammar_keyword_census | DRAFT-MANUAL | yes (is) | neither | human request |
| //parser-equivalence:migration_sizing | DEAD | — | — | none (reason gone, `b23f68757`) |
| //parser-equivalence:pmcd_reachability_census | DRAFT-MANUAL | yes (is) | neither | human request (upstream jars, roster) |
| //pct:adapter_par | BUILD-OUTPUT (testonly; fix timestamps) | no | none | `pct/src/main/resources`, ParGenerator, upstream jars |
| //pct:ratchets | COMMITTED-UPSTREAM | yes (is; leave root writer) | bump-only + explicit re-pin | upstream release; secondary: our parser/compiler walls |
| //tools/engine-runner:vocab | DEAD (or COMMITTED-UPSTREAM if kept as a record) | yes | bump-only if kept | @maven_runner release |
| //scripts/parser:keyword_coverage | COMMITTED-SOURCE (mixed) | yes | everyday `//:update_generated` (+ bump) | our `.pure`; engine grammars; keywords.py/tiers.py |
| //tools/reference:ref_dump | BUILD-OUTPUT (testonly) | yes (is) | none | upstream jars; RefResolutions.java |
| //tools/reference:ref_imports | DRAFT-MANUAL | yes (is) | none | human request |
| //tools/java_run:pins | BUILD-OUTPUT (make testonly) | no | none | `tools/java_run/defs.bzl`, PrintPins.java, JDK |
