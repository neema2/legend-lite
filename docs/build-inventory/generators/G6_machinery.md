# G6: the machinery around the generators

Repo: `the build/rebuild checkout`, branch `build/rebuild`, HEAD `669b39ad1`. Read-only. Every
query below was run with `env -C <repo> bazel ...` on 2026-10-05. `R:` = root `BUILD.bazel`. `bazel_lib` is 3.7.2
(`MODULE.bazel:23`); its sources were read from the output base (`external/bazel_lib+/lib/...`).

---

## 0. How the machinery works (bazel_lib 3.7.2, read from source)

- `write_source_files(name, files = {...})` (`lib/write_source_files.bzl:109-223`):
  - **one file:** the writer is `name` and its diff test is `name_test`;
  - **several files:** one writer `name_<i>` per file, numbered in the dict's order, with diff test `name_<i>_test`.
    `name` is then an umbrella writer with no file of its own, whose `additional_update_targets` are the `name_<i>`
    (`:217-223`);
  - in both cases the diff tests are grouped in a `test_suite` called `name_tests`, which carries the call's `tags`
    (`:209-215`);
  - `diff_test = False` makes no test and no suite (`private/write_source_file.bzl:116-117`).
- **A writer is an executable script** (`private/write_source_file.bzl:223-311`). It `cp`s each input file over its
  path in the checkout, under `$BUILD_WORKSPACE_DIRECTORY`, then runs each `additional_update_targets` script in list
  order (`:298-303`). The writer's runfiles hold its input files and the runfiles of everything it lists (`:440-447`).
  So **`bazel run <writer>` builds every generator under it before it copies anything**: all of them see the checkout
  as it was when the run started. No generator ever sees another one's freshly written file in the same run.
- **The diff test** (`private/diff_test.bzl`, `diff_test_tmpl.sh:74-82`) runs `diff build-output committed-file`.
  - On a mismatch it prints the diff to the log, then
    `FAIL: files "<a>" and "<b>" differ. <failure message>`.
  - On Windows it runs `fc.exe` and prints only "differ", the message, and "To see differences run: diff <runfiles
    paths>" (`diff_test_tmpl.bat:80-106`).
- **The failure message** is `diff_test_failure_message`, in which `{{DEFAULT_MESSAGE}}` expands to one of two texts
  (`private/write_source_file.bzl:160-181`):
  - **one file:** "`<file> is out of date. To update this file, run: bazel run //pkg:name`";
  - **several files:** "`To update this and other generated files, run: bazel run @@//pkg:name` … `To update *only*
    this file, run: bazel run //pkg:name_<i>`". The umbrella is printed through `str(Label)`, hence the `@@//`.
  - No package sets `suggested_update_target`, so no default message names `//:update_generated`.
- **A committed file that is missing** turns its diff test into a `fail_with_message_test`, decided by a glob at load
  time (`:119-158`).

---

## 1. Every writer and diff test

Query: `kind("_write_source_file|_diff_test", //...)` returns **71 `_write_source_file`** and **60 `_diff_test`**,
matching `runs/generators/all.txt`. The 60 tests are 59 made by `write_source_files` plus the plain
`//tools/java_run:pins_test`.

### 1a. The 18 `write_source_files` calls, file by file

"In G?" means the diff test is in `//:generated` (query `tests(//:generated)`, 47 tests). "In U?" means the writer is
reached from `//:update_generated` (query `kind(_write_source_file, deps(//:update_generated))`, 55 targets).

| Call (BUILD:line) | File written (index) | Generator | Suite | In G? | In U? |
|---|---|---|---|---|---|
| `//core:update_generated` (core/BUILD.bazel:782; files `_GENERATED` :699-706) | `_0` builtin/DynaFn.java · `_1` builtin/Pure.java · `_2` compiler/NameResolver.java · `_3` engine-handlers.tsv · `_4` native-claims.tsv · `_5` prelude.pure | `//spec:gen_dynafn`, `gen_natives`, `gen_imports`, `gen_engine_handlers`, `gen_claims`, `gen_prelude` | `//core:update_generated_tests` | Y | Y |
| `//core:update_ladder` (core/BUILD.bazel:821, testonly) | `_0`…`_11`: `src/test/resources/ladder/r01…r12.current.sql` | `//core:ladder_report` | `//core:update_ladder_tests` | Y | Y |
| `//core:update_stress_corpus` (core/BUILD.bazel:769) | `_0`…`_9`: stress/59, 60, 64 (dense), 92-98 (stress) · `_10` stress-layout.json | `//scripts/corpus:gen_dense`, `gen_stress`; `//core:stress_layout` | `//core:update_stress_corpus_tests` | Y | Y |
| `//core:draft_native_membership` (core/BUILD.bazel:832, `diff_test = False`) | native-membership.tsv (hand-owned) | `//spec:native_membership_draft` | none | – | N |
| `//datacube:update_generated` (datacube/BUILD.bazel:421) | `_0` src/generated/offer-facts.ts · `_1` src/generated/catalog-facts.ts · `_2` test/generated/catalog-corpus.ts · `_3` test_imports.bzl | `:offer_facts` (fed by `:offer_queries`), `:catalog_rules`, `:catalog_corpus`, `:test_imports` | `//datacube:update_generated_tests` | Y | Y |
| `//datacube:cut_link_dictionary` (datacube/BUILD.bazel:316, `diff_test = False`, `check_that_out_file_exists = False`) | src/share/link-p2.ts (does not exist yet) | `:link_dictionary_next` | none | – | N |
| `//docs:update_generated` (docs/BUILD.bazel:11, testonly) | protocol-roster.tsv | `//parser-equivalence:gen_roster` | `//docs:update_generated_tests` | Y | Y |
| `//docs:draft_own_corpus_ledger` (docs/BUILD.bazel:27, `diff_test = False`) | own-corpus-protocol-diffs.tsv (hand-owned) | `//parser-equivalence:gen_own_corpus_draft` | none | – | N |
| `//engine-client:update_generated` (engine-client/BUILD.bazel:55) | src/generated/lite-facts.ts | `:lite_facts` | `//engine-client:update_generated_tests` | Y | Y |
| `//fixtures/saved-queries:update_generated` (BUILD.bazel:64) | `_0`…`_3`: data-space-context, default-parameter-values, explicit-context, graph-fetch `.json` | `:gen` | `…:update_generated_tests` | Y | Y |
| `//legend-art:update_generated` (legend-art/BUILD.bazel:111) | src/icons.ts | `:icons_gen` | `//legend-art:update_generated_tests` | Y | Y |
| `//parser-equivalence:update_generated` (BUILD.bazel:349) | `_0` corpus-manifest.tsv · `_1` engine-grammar-fixtures.jsonl | `:gen_manifest`, `:gen_fixtures` | `…:update_generated_tests` | Y | Y |
| `//parser-equivalence:update_ratchets` (BUILD.bazel:382) | com/legend/equivalence/ratchets.tsv | `:ratchets` | `…:update_ratchets_tests` | Y | Y |
| `//pct:update_ratchets` (pct/BUILD.bazel:274, **tagged manual**) | channelb/ratchets.tsv | `//pct:ratchets` (**manual**) | `//pct:update_ratchets_tests` (manual) | **N** | **Y** |
| `//scripts/parser:update_keyword_coverage` (BUILD.bazel:96) | keyword-coverage.tsv | `:keyword_coverage` | `…:update_keyword_coverage_tests` | Y | Y |
| `//spec:update_ratchets` (spec/BUILD.bazel:407) | com/legend/generators/ratchets.tsv | `//spec:ratchets` | `//spec:update_ratchets_tests` | Y | Y |
| `//spec:update_rcorpus_duckdb` / `_h2` (spec/corpus.bzl:139, made by `corpus_lane` at spec/BUILD.bazel:156, :196) | `_0` fail-roster · `_1` skipped-roster · `_2` unordered-register · `_3` engine-order-register (host pass) · `_4` database-engine-order-register (database pass), per lane | `//spec:judge_host_<lane>`, `judge_database_<lane>` | `//spec:update_rcorpus_<lane>_tests`, inside the lane suite `//spec:corpus_<lane>` (corpus.bzl:151-156) | **N** | **N** |
| `//spec:update_reference_lane` (spec/BUILD.bazel:262, **manual**) | reference-lane/core_relational.txt | `//spec:reference_lane_report` (manual) | `//spec:update_reference_lane_tests` (manual) | **N** | **N** |
| `//tools/engine-runner:update_vocab` (BUILD.bazel:71) | vocab.tsv | `:vocab` | `…:update_vocab_tests` | Y | Y |
| `//warehouse:update_reachability_metadata` (BUILD.bazel:398) | META-INF/native-image/com.legend/warehouse/reachability-metadata.json | `:reachability_metadata` | `…:update_reachability_metadata_tests` | Y | Y |
| `//:update_generated` (R:105) | none (an umbrella of 15) | – | none | – | itself |

The one diff test outside `write_source_files` is `//tools/java_run:pins_test` (tools/java_run/BUILD.bazel:25). It is
a plain `diff_test` of `:pins` against the hand-kept `pins/pins.expected`, with no writer. It is in the CI checks lane
and in `//gates:local`, not in `//:generated`.

### 1b. Writers, diff tests and suites outside the root targets

There are 18 suites (query `attr(name, "^(update_|draft_|cut_).*_tests$", kind(test_suite, //...))`). `//:generated`
lists 14 of them (R:79-97). The 4 it leaves out:

| Suite | Contains | Run by |
|---|---|---|
| `//pct:update_ratchets_tests` | 1 test (manual) | nothing: it is in no suite (`rdeps(//..., …, 1)` shows only its own suite) and in no CI lane. `pct/BUILD.bazel:280` says gate 9 holds the file instead: the Channel B tests compare live discovery with `PctRatchets.measured(...)` |
| `//spec:update_rcorpus_duckdb_tests` | 5 tests | `//spec:corpus_duckdb` (CI lane 4) |
| `//spec:update_rcorpus_h2_tests` | 5 tests | `//spec:corpus_h2` (CI lane 5) |
| `//spec:update_reference_lane_tests` | 1 test (manual) | nothing: no lane, no suite. `ReferenceLaneTest.java:27` says to run it by hand |

`//:update_generated` lists 15 writers (R:108-124). It reaches 55 of the 71 `_write_source_file` targets. The 16 it
does not reach (query `kind(_write_source_file, //...) except kind(_write_source_file, deps(//:update_generated))`):

- `//core:draft_native_membership`, `//datacube:cut_link_dictionary` and `//docs:draft_own_corpus_ledger`: drafts,
  left out on purpose (core/BUILD.bazel:829-831, datacube/BUILD.bazel:304-308, docs/BUILD.bazel:21-25);
- `//spec:update_rcorpus_duckdb` and `_h2`, with their 10 members: left out on purpose (R:102-104, P2-15 and D9);
- `//spec:update_reference_lane`: manual.

**Which root target covers what:**

- **In both:** 14 writer groups.
- **Only in `//:update_generated`:** `//pct:update_ratchets`. It is written by every update, but its diff test runs
  nowhere.
- **Only in a lane:** the rcorpus diff tests, which run in lanes 4 and 5 and are written by hand.
- **In neither:** the reference lane, and the three drafts.

Nothing checks that a new suite or writer is added to the root targets. "A missing suite fails the build" (R:78) is
true only for a suite that is listed and then deleted (the label stops resolving). Nothing catches a new package's
suite that was never listed. `grep generated tools/guards/*` finds no such guard. The design's "a guard checks that
every diff test belongs to a group" (BUILD_REBUILD_DESIGN §4.2, Mechanics) does not exist yet. Today the only safety
net is `bazel test //...`, and no CI lane runs that (§6).

---

## 2. `//:update_generated` exactly

**What `bazel run //:update_generated` does.**

1. Bazel builds the writer's runfiles: every input file of the 55 writers below it.
2. The script (`_update.sh`) runs. The root writer has no file of its own, so it runs each listed writer's script in
   `additional_update_targets` order (R:108-124):
   - `//core:update_generated` (`_0` … `_5`)
   - `//core:update_ladder` (`_0` … `_11`)
   - `//core:update_stress_corpus` (`_0` … `_10`)
   - `//datacube:update_generated` (`_0` … `_3`)
   - `//engine-client`, then `//docs`
   - `//fixtures/saved-queries` (`_0` … `_3`)
   - `//parser-equivalence:update_generated` (`_0`, `_1`)
   - `//legend-art`, then `//parser-equivalence:update_ratchets`
   - `//pct:update_ratchets`
   - `//scripts/parser:update_keyword_coverage`
   - `//spec:update_ratchets`
   - `//tools/engine-runner:update_vocab`
   - `//warehouse:update_reachability_metadata`

The order changes nothing, because every generator has already run against the checkout as it was before the run.

**The 48 files it writes** come from a cquery of the runfiles,
`--starlark:expr=providers(target)["DefaultInfo"].default_runfiles.files`, listing the non-source files: 29 core, 4
datacube, 1 docs, 1 engine-client, 4 saved-queries, 1 legend-art, 3 parser-equivalence, 1 pct, 1 scripts/parser, 1
spec, 1 vocab and 1 warehouse.

**The generators it builds.** The query
`kind("_java_run|_run_binary|stress_index|stress_layout|file_list|jar_entry|java_jars", deps(//:update_generated))`
returns 31 targets:

- **Spec chain:** `//spec:gen_claims`, `gen_dynafn`, `gen_engine_handlers`, `gen_imports`, `gen_natives`,
  `gen_prelude`, `ratchets`.
- **Parser-equivalence:** `gen_fixtures`, `gen_manifest`, `gen_roster`, `ratchets`, `engine_jars_exec`,
  `harvest_tests_jars`.
- **PCT:** `//pct:ratchets` (**manual**) and `//pct:adapter_par`, which `pct:ratchets` pulls in through
  `pct_tests_lib → adapter → adapter_par_jar`.
- **Core:** `//core:ladder_report`, `stress_layout` and `stress_index`. `stress_index` is a resource of
  `core_tests_lib`.
- **DataCube:** `offer_queries`, `offer_facts`, `catalog_rules`, `catalog_corpus`, `test_imports`.
- **Others:** `//engine-client:lite_facts`, `//fixtures/saved-queries:gen` (starts the server),
  `//legend-art:icons_gen`, `//scripts/corpus:gen_dense`, `gen_stress`, `//scripts/parser:keyword_coverage`,
  `//tools/engine-runner:vocab` (**twice**, see below) and `//warehouse:reachability_metadata`.

**The one manual generator it pulls in is `//pct:ratchets`.** Query: `somepath(//:update_generated, //pct:ratchets)`
→ `//pct:update_ratchets → //pct:ratchets`. `pct:ratchets` takes `memory_mb = 2048` and computes every Channel B
suite's discovery (pct/BUILD.bazel:258-270), and its PAR dependency `adapter_par` takes 4096 MB (pct/BUILD.bazel:48-49).
The root writer is neither manual nor tagged, so `bazel build //...` and `bazel test //...` build its runfiles and run
`pct:ratchets` too (§6).

**Vocab is built twice, and core compiled a second time.**

- `bazel aquery "outputs('.*engine-runner/generated/vocab.tsv', deps(//:update_generated))"` returns two Generate
  actions: `darwin_arm64-fastbuild` and `darwin_arm64-opt-exec`.
- The exec copy comes from `//scripts/parser:keyword_coverage`. Its `tool = ":keywords"` (scripts/parser/BUILD.bazel:93)
  is a `py_binary` whose `data` includes `//tools/engine-runner:vocab` (:28). The `RunBinary` action's inputs include
  `bazel-out/darwin_arm64-opt-exec/bin/scripts/parser/keywords.runfiles` (aquery), and keywords' runfiles contain
  `vocab.tsv` (cquery starlark). Building them compiles `:runner` and all of `//core` in the exec configuration:
  40 `Javac` actions in `opt-exec` under `deps(//scripts/parser:update_keyword_coverage_test)`, and 41 under
  `deps(//:update_generated)`.
- Commit `b245f5f30` says "The run_binary no longer declares the vocab it never reads (the report keeps it)". The vocab
  still enters through the tool's runfiles.
- Every other generator is built once. For example, `outputs('.*spec/generated/DynaFn.java', …)` gives the same
  artifact `bazel-out/darwin_arm64-fastbuild/bin/spec/generated/DynaFn.java` under `//:update_generated`, under
  `//core:update_generated_0_test` and at top level. The design's R4, "It also builds every generator a second time",
  is therefore wrong in general and right only for vocab and the exec-config core.

### 2a. Does one run reach a fixed point? No

A generator that reads another generator's **committed** copy, instead of its build output, sees stale input after any
run that changed that copy. These are the reads, found in each program's source:

| Generator | Committed copy it reads (another generator's output) | How (evidence) |
|---|---|---|
| `gen_natives` | `prelude.pure` (gen_prelude's output) | `NativesGenerator.java:328` calls `NameResolver.resolve`. NameResolver's static initialiser runs `PLATFORM_TYPE_FQNS = computePlatformTypeFqns()` (`NameResolver.java:298-305`) and `PLATFORM_FQNS` (:315). Both read `Prelude.classFqns()/elements()`, and `Prelude` loads `/com/legend/builtin/prelude.pure` from the classpath, which is `//core`'s committed copy (`Prelude.java:39-44`). Whether this changes Pure.java's bytes: **OPEN** |
| `gen_dynafn` | `engine-handlers.tsv` (gen_engine_handlers), compiled `Pure.java` (gen_natives) | `DynaFnGenerator.java:106` `com.legend.builtin.EngineHandlers.fqnsOf(...)` reads the classpath resource (`EngineHandlers.java:73`). `:97-99` reads the `Pure.SQL_NULL/TRUE/FALSE` constants. The class has no `import com.legend` and uses fully qualified names, so the design's R2 table ("`spec:gen_dynafn`, `gen_imports` \| no core class at all (0 `com.legend` imports)") is **wrong for gen_dynafn** |
| `gen_engine_handlers` | compiled `Pure.java` (gen_natives), `prelude.pure` (gen_prelude) | `EngineHandlersGenerator.java:82` `Pure.all()`, `:85` `Prelude.elements()`, `:110-112` `Pure.LITE_SURFACE`. It declares only Handlers.java (spec/BUILD.bazel:316-328) |
| `gen_prelude` | compiled `Pure.java`, compiled `NameResolver.CORE_IMPORTS` (gen_imports), the Claims registry over the **committed** core, DynaFn.java and NameResolver.java as committed text, `prelude.pure` | `PreludeGenerator.java:436, 556, 583` `Pure.nativeFunctionsAt`. `:429` and `:541` `Claims.claimedBareNames()`, through `//spec:claims`, which is built on `//core` (spec/BUILD.bazel:74-86). `:321, :475` `NameResolver.CORE_IMPORTS`. Only `Pure.java` is overridden with the chain's output (spec/BUILD.bazel:462); the rest of `//core:main_java` is the committed text. It also triggers NameResolver's prelude-reading initialiser |
| `gen_claims` | runs on `//core:core_next`, built from the generated Java and the generated prelude (consistent within a run), but its classpath also carries the committed `engine-handlers.tsv` and `native-claims.tsv` (core/BUILD.bazel:861-864 excludes only prelude.pure) | No direct reference in `Claims.java`, `ClaimsGenerator.java`, `RegistryKeys`, `CoreFn` or `NativeFn` (grep). Whether a static initialiser reaches EngineHandlers: **OPEN** |
| `spec:ratchets` | compiled `DynaFn.java` (gen_dynafn) | `SpecRatchets.java:44-45` `DynaFn.withResolution(UNSUPPORTED).size()` |
| every generator built on `//core` (`ladder_report`, `lite_facts`, `catalog_*`, `offer_facts`, `saved-queries:gen`, `vocab`, the parser-equivalence generators, `pct:ratchets`) | the five committed core files, compiled or as resources | the query `deps(g) intersect set(<the 59 committed files>)` lists `DynaFn.java Pure.java NameResolver.java engine-handlers.tsv prelude.pure` for each one (§2b) |

**The chain therefore contains a cycle through committed files**: `gen_natives` reads the committed `prelude.pure`,
and `gen_prelude` reads the compiled committed `Pure.java`. The longest path is: `Pure.java` (run 1) →
`engine-handlers.tsv` and `prelude.pure` (run 2) → `engine-handlers.tsv` again (it reads the prelude) and `DynaFn.java`
(it reads engine-handlers) (run 3) → `DynaFn.java` and `spec ratchets.tsv` (run 4). After an upstream change that
moves Pure.java, one `bazel run //:update_generated` is not enough. The `//:generated` diff tests catch what is left,
and their messages say "bazel run //:update_generated" again, so a person converges by repeating it.

- **Already raised:** area2_report.md:59-63 and design §7 have this as OPEN, citing gen_engine_handlers and the Claims
  step only. It is wider than that: it also takes in gen_dynafn, gen_natives (through NameResolver),
  `spec:ratchets` and every core-based generator.
- **Not affected:**
  - parser-equivalence passes the harvest's **build output** to `gen_manifest` and `gen_roster`
    (`_NEW_FIXTURES`, parser-equivalence/BUILD.bazel:301; `Corpus.java:179-185`);
  - the stress generators read `//core:stress_layout`'s build output, not the committed JSON
    (scripts/corpus/BUILD.bazel:77-78);
  - `datacube/BUILD.bazel:21` loads the committed `test_imports.bzl` at load time, but its generator reads only the TS
    imports. That is not a cycle.

### 2b. Even with unchanged content, the next build reruns generators (their own committed output is an input)

`deps(g) intersect set(<committed files>)` for each generator shows inputs that `//:update_generated` itself rewrites.
After an update that changed them, the next `bazel build` or `bazel test` reruns the generator even when its output
bytes come out the same:

- **Its own output, as input:**
  - `//core:ladder_report` takes the 12 `.current.sql` pins, through `core_tests_lib` resources (core/BUILD.bazel:318).
  - `//spec:ratchets` takes `spec ratchets.tsv`, through `spec_tests_lib` resources (spec/BUILD.bazel:94).
  - `//pct:ratchets` takes `pct ratchets.tsv` (pct/BUILD.bazel:81).
  - `//spec:gen_claims` takes `native-claims.tsv`, through core_next's resources.
  - `//parser-equivalence:gen_fixtures`, `gen_manifest`, `ratchets` and `gen_roster` take `corpus-manifest.tsv`,
    `engine-grammar-fixtures.jsonl` and `ratchets.tsv`, through `pe_tests_lib` resources (parser-equivalence/BUILD.bazel:51).
  - `//warehouse:reachability_metadata` takes its own JSON.
- **Other generators' outputs, through whole-tree filegroups:**
  - `gen_roster` and `parser-equivalence:ratchets` take the stress files, the ladder pins, `spec` and `pct`
    `ratchets.tsv`, all 10 rcorpus rosters and the reference-lane golden, through `//core:srcs`, `//spec:srcs` and
    `//pct:srcs` (parser-equivalence/BUILD.bazel:329-331, :368-370).
  - **The corpus passes** `judge_host_*` and `judge_database_*` take the ladder pins, the stress files,
    `spec ratchets.tsv` and the reference golden, through `//core:srcs` (spec/BUILD.bazel:122-125) and `spec_tests_lib`.
    So an update that moves a spec ratchet or a ladder pin reruns both corpus lanes (H2's pass takes 4 GB). Commit
    `e8dcffb38` says "an update never reruns the corpus", but that holds only for the rosters.

---

## 3. `//:generated` exactly

- **What it is:** 14 suites, 47 diff tests (R:79-97).
  - **29 core:** 6 spec-chain, 12 ladder, 11 stress.
  - **4 datacube.**
  - **1 each:** docs, engine-client, legend-art.
  - **4 saved-queries.**
  - **3 parser-equivalence:** 2 generated files and ratchets.
  - **1 each:** keyword coverage, spec ratchets, vocab, reachability metadata.
- **What building it executes:** 29 generators, from `deps(//:generated) intersect <generators>`. Each diff test has
  the generator's output in its runfiles, so all of them run:

  | Package | Generators |
  |---|---|
  | `//spec` | the six `gen_*`, `ratchets` |
  | `//parser-equivalence` | `gen_fixtures`, `gen_manifest`, `gen_roster`, `ratchets`, `engine_jars_exec`, `harvest_tests_jars` |
  | `//core` | `ladder_report`, `stress_layout`, `stress_index` |
  | `//datacube` | `offer_queries`, `offer_facts`, `catalog_rules`, `catalog_corpus`, `test_imports` |
  | others | `//engine-client:lite_facts`, `//fixtures/saved-queries:gen`, `//legend-art:icons_gen`, `//scripts/corpus:gen_dense`, `gen_stress`, `//scripts/parser:keyword_coverage`, `//tools/engine-runner:vocab` (target and exec), `//warehouse:reachability_metadata` |

  On top of these it compiles `//core:core_next` (749 files, for `gen_claims`) and, through the keywords tool's
  runfiles, all of core again in `opt-exec`.
- **What an engine edit costs the checks lane and `//gates:local`.**
  - **Exec-layer edit.** The query `rdeps(<generators>, //core:src/main/java/com/legend/exec/AssertListener.java)`
    intersected with the set above leaves **20 generators that rerun**: `ladder_report`, `catalog_corpus` (runs
    DuckDB), `catalog_rules`, `offer_facts`, `lite_facts`, `saved-queries:gen` (starts the server), `gen_fixtures`,
    `gen_manifest`, `gen_roster` and `engine_jars_exec`, `parser-equivalence:ratchets`, `keyword_coverage` (through
    the exec vocab), `vocab`, the six spec `gen_*`, and `spec:ratchets`. Add a core_next recompile and an exec-config
    core recompile.
  - **Unaffected:** the stress generators (Python), `icons_gen`, `test_imports`, `offer_queries` (TypeScript) and
    `reachability_metadata`.
  - **Even when the generator reads no core:** `gen_imports` reruns, because `:generators` depends on `//core`
    (spec/BUILD.bazel:63-67), although `ImportsGenerator` imports nothing from core.
  - **Parser edit** (`//core:src/main/java/com/legend/parser/DatabaseProtocolParser.java`): the same 20, plus
    DataCube's and wasm's JVM answers, which are outside `//:generated`.
- **On three platforms:** the checks lane runs on Linux, macOS and Windows (gate.yml:61-99). So every one of these 29
  generators must give the same bytes on all three, because each diff test compares that platform's build against a
  single committed copy.

---

## 4. The bump (tools/bump/Bump.java)

The usage is `bazel run //tools/bump -- <release> [--pins]` (:25-26, :83-86). The program needs
`BUILD_WORKSPACE_DIRECTORY` (:79-81).

1. **Phase 0, decide** (:93-117).
   - Reads `release.MODULE.bazel`'s PINS block (:94-96, `readPins` :174-190).
   - Downloads the engine pom from Maven Central, or fails (:98-100).
   - Derives the pure release from `<legend.pure.version>` (:101) and checks the pure pom exists (:102-103).
   - Resolves both tag commits over HTTPS from GitHub's ref advertisement (:107-108, :295-302).
   - Reads 5 engine-managed versions from the pom (:111-116).
   - Runs no Bazel command.
2. **Phase 1, move** (:119-146).
   - Downloads both source archives to compute their integrity (:128, :130, :277-291).
   - Rewrites the PINS block whole (:132, `writePins` :194-216).
   - For each pool in `RELEASE_POOLS = [maven_upstream, maven_runner]` (:70), runs **`REPIN=1 bazel run
     @<pool>//:pin`** (:139-142).
   - `--pins` stops here (:143-146).
3. **Phase 2, regenerate:** **`bazel run //:update_generated`** (:148-151). This is the only writer command the bump
   runs. On failure it says "a generator REFUSED … fix the platform, then re-run (the bump is idempotent)".
4. **Phase 3, check:** **`bazel test //...`** (:153-157). On failure it says "gates are red: that is the judgement
   half — … re-pin every moved ratchet with a reason".
5. It prints NEXT: read the diff (it names `prelude.pure / Pure.java / native-*.tsv / DynaFn.java /
   corpus-manifest.tsv / protocol-roster.tsv / the fixture snapshot`), re-pin ratchets, commit (:159-167).

Every Bazel call is `$BAZEL_REAL`, else `bazel`, with no `--config=ci` (:304-316).

**The writers it reaches** are the 55 under `//:update_generated` (48 files, §2). Among them is the manual `//pct:ratchets`,
and `pct:update_ratchets` exists in the root list for this reason ("a bump moves discovery", pct/BUILD.bazel:280,
commit `b245f5f30`).

**What it never writes:**

- **The rcorpus rosters:** phase 3 fails on the corpus lanes if they moved. That is by design ("a decision", R:102-104).
- **The reference-lane golden:** manual, and in no phase-3 test, so **a bump never checks it**.
- **The drafts:** gate 8's `OwnCorpusParityTest` fails if the own-corpus ledger moved.

**What it assumes:**

- **One pass is a fixed point.** It runs `//:update_generated` once. "Idempotent" (:41-42, :150) means a re-run of the
  whole bump gives the same result, not that one pass converges. By §2a it does not always converge. A non-converged
  file then shows up in phase 3 as a red `//:generated` diff test, and the bump's message files it under "the
  judgement half".
- **"Re-pin every moved ratchet" reaches the gates.** It does not for the measured ratchets: phase 2 has already
  rewritten `spec`, `parser-equivalence` and `pct` `ratchets.tsv` and the ladder pins, so phase 3 compares live values
  with the copies just written and stays green. Those moves show only in `git diff`. Only the hand-owned ceilings
  (Java constants, D9 (b)) can redden a gate.
- **What `bazel test //...` covers.** It also builds every non-manual non-test target, so phase 3 executes every
  non-manual generator, `pct:ratchets` again through `//:update_generated`, and both corpus lanes. It skips the manual
  tests: `//pct:update_ratchets_test` and `//spec:update_reference_lane_test`.

**Which generators truly depend on the upstream release.** Query `rdeps(<generators>, @<repo>//...:*) intersect
<generators>`, one per repository; the generators list is runs/generators/all.txt's non-writer entries:

| Repository | Generators that depend on it |
|---|---|
| `@legend_engine_src` (28) | parser-equivalence `corpus_census`, `gen_fixtures`, `gen_manifest`, `gen_own_corpus_draft`, `gen_roster`, `grammar_keyword_census`, `migration_sizing`, `pmcd_reachability_census`, `ratchets`; `pct:ratchets`; `scripts/parser:keyword_coverage`; spec `eager_corpus_compile`, `_world2`, `gen_claims`, `gen_dynafn`, `gen_engine_handlers`, `gen_imports`, `gen_natives`, `gen_prelude`, the six `judge_*`, `native_declarations`, `ratchets`, `reference_lane_report` |
| `@legend_pure_src` (25) | the same, minus `keyword_coverage`, `gen_engine_handlers` and `gen_imports` |
| `@oracle_pins` (10, through `//tools:oracle-pins.env`) | the 9 parser-equivalence programs above; `spec:reference_lane_report` |
| `@maven_upstream` (17) | parser-equivalence's 9 above, plus `engine_jars`, `engine_jars_exec` and `harvest_tests_jars`; `pct:adapter_par`; `pct:ratchets`; `spec:reference_lane_report`; `tools/reference:ref_dump` and `ref_imports` |
| `@maven_runner` (2) | `tools/engine-runner:vocab`; `scripts/parser:keyword_coverage`, through the exec vocab |

Seven of the 15 writer groups under `//:update_generated` have **no** dependency on the Legend release:

- `//core:update_ladder` and `//core:update_stress_corpus`;
- `//datacube:update_generated` and `//engine-client:update_generated`;
- `//fixtures/saved-queries:update_generated`;
- `//legend-art:update_generated` (its pin is `@react_icons`);
- `//warehouse:update_reachability_metadata`.

R:99-101 ("EVERY generated file … from the pinned upstream release") and README.md:294 say otherwise.

---

## 5. Humans: every instruction to run a writer or generator

Search: `git grep -E 'bazel run //…:(update_|draft_|cut_link)|update_generated|update_ratchets|update_ladder|update_rcorpus|update_vocab|update_keyword|update_reachability|update_stress|update_reference_lane'`,
excluding BUILD and .bzl files, which are covered in §1 and §7, and plan-audit history. **STALE** marks a wrong
instruction.

| Where | Says | Status |
|---|---|---|
| README.md:294 | `bazel run //:update_generated # regenerate every generated file from the pinned upstream release` | **STALE / imprecise**: it leaves out rcorpus, the reference lane and the drafts, and 7 of the 15 groups do not come from upstream (§4) |
| README.md:293 | `bazel test //... # every gate, and every generated file checked against its generator` | imprecise: not the manual pct and reference diff tests |
| docs/GATES.md:39-47 | `//:generated` "is `//core:update_generated_*_test`, … `//query:update_generated_test` …" (lists DataCube's lite-facts.ts and Query's icons.ts); "Regenerate: `bazel run //:update_generated`" | **STALE**: `//query` has no writer (it moved to `//legend-art` in `80ee5faa1`); lite-facts is in `//engine-client`. It leaves out engine-client, legend-art, keyword coverage, vocab, both ratchets and reachability |
| docs/GATES.md:51 (checks row) | "`//:generated` holds each package's diff-test suite; a missing suite fails the build" | misleading: nothing catches an **unlisted** suite (§1b) |
| docs/GATES.md:71-74 | `//docs:draft_own_corpus_ledger`, `//core:draft_native_membership`, `//datacube:cut_link_dictionary` are DRAFT writers; `bazel build //spec:native_declarations` | correct |
| docs/GATES.md:77 | a moved roster is re-blessed by `bazel run //spec:update_rcorpus_<lane>` | correct |
| docs/GATES.md:83-85 | `bazel run //tools/bump -- <release>` moves it: "pins, repin, regenerate, every gate" | correct |
| release.MODULE.bazel:18-21 | bump = … "regenerates every generated file and runs every gate; then the judgement half (docs/UPSTREAM_BOUNDARY_HOMEWORK_2026_09_10.md §5 phases 3-6)" | **STALE target**: that §5 is Maven-era. It cites `mvn -pl core test -Dprelude.generate=1`, `tools/allgates.sh`, `tools/diagnostics.sh` and `tools/version-report.sh`; none of the three `tools/*.sh` exists (ls) |
| docs/GATES.md:1077 | cites UPSTREAM_BOUNDARY_HOMEWORK §5 | same, stale target |
| tools/bump/Bump.java:40-45, :163-167 | phase 2 = "every generated file"; NEXT names "native-*.tsv" | imprecise: native-membership.tsv is hand-owned (core/BUILD.bazel:829) |
| core/src/main/java/com/legend/builtin/DynaFn.java:32, Prelude.java:25, compiler/NameResolver.java:212 | `bazel run //:update_generated` | correct |
| core/src/main/resources/…/engine-handlers.tsv:1, native-claims.tsv:1, prelude.pure:4 (written by EngineHandlersGenerator.java:95, ClaimsGenerator.java:208, PreludeGenerator.java:647) | "regenerate: bazel run //:update_generated" | correct, but one run may not converge (§2a) |
| core/BUILD.bazel:791; LadderRender.java:30; LeanSqlLadderTest.java:23 | "`//core:update_ladder_test` (in //:generated)" | **STALE name**: the tests are `update_ladder_<0..11>_test`, in the suite `update_ladder_tests` |
| LeanSqlLadderTest.java:38 | "no current pin (bazel run //core:update_ladder)" | correct |
| core/src/test/resources/stress/93-testdata.pure:6; scripts/corpus/build.py:4, :56, :382; model.py:51 | `bazel run //core:update_stress_corpus` | correct |
| scripts/corpus/dense_build.py:10 | "//core:update_generated writes the files back and diff-tests them" | **STALE**: it is `//core:update_stress_corpus` |
| core/src/test/java/com/legend/builtin/EngineHandlersTest.java:54 | "//core:update_generated's" | correct |
| datacube/src/calc.ts:107 (an error message) | `no compiler facts for '…': bazel run //datacube:update_generated` | correct |
| datacube/src/generated/*.ts, test/generated/catalog-corpus.ts, CatalogFacts.java:45, :63, OfferFacts.java:49, :237, test-imports.mts:39 | `bazel run //datacube:update_generated` | correct |
| datacube/src/grid/columns.ts:44-46 | "`src/generated/lite-facts.ts` … fails `bazel test //datacube:update_generated_test`" | **STALE**: lite-facts is `engine-client/src/generated/lite-facts.ts`, and no such datacube test name exists |
| engine-client/tools/typefacts/TypeFacts.java:26-27 | "Built by `//datacube:type_facts`; … (`bazel run //datacube:update_generated`)" | **STALE**: it is `//engine-client:lite_facts` and `//engine-client:update_generated` (TypeFacts.java:57 writes the right one) |
| engine-client/README.md:14, src/generated/lite-facts.ts:2 | `bazel run //engine-client:update_generated` | correct |
| datacube/test/share-link.test.ts:98, tools/link-dictionary/make.ts:12, docs/DATACUBE_SAVE_SHARE_2026_09_28.md:254 | `bazel run //datacube:cut_link_dictionary` | correct |
| fixtures/saved-queries/README.md:33, make.mjs:6 | `bazel run //fixtures/saved-queries:update_generated` | correct |
| legend-art/README.md:20, src/icons.ts:2, tools/icons.mjs:5, :152 | `bazel run //legend-art:update_generated` | correct |
| parser-equivalence Corpus.java:201 (an error) | "engine fixture snapshot … missing — regenerate it: bazel run //:update_generated" | correct |
| RosterGenerator.java:43 (written into docs/protocol-roster.tsv:4) | "Regenerate: bazel run //:update_generated." | correct |
| PeRatchets.java:39, :53, :64, :80; MutationFuzzTest.java:139; OwnCorpusParityTest.java:84; its ratchets.tsv:1 | `bazel run //parser-equivalence:update_ratchets` | correct |
| OwnCorpusLedgerDraft.java:15, :61; OwnCorpusParityTest.java:37, :80; docs/own-corpus-protocol-diffs.tsv:4 | `bazel run //docs:draft_own_corpus_ledger` | correct |
| PctRatchets.java:43, :57, :68, :83; ChannelB{Essential:66, Grammar:78, Relation:94, Standard:80, Unclassified:71}Test (assert messages); its ratchets.tsv:1 | `bazel run //pct:update_ratchets` | correct (2 GB; also run by every root update) |
| SpecRatchets.java:52, :66, :77, :92, :107; DynaFnRegistryTest.java:105; UpstreamPathManifestTest.java:110; MinimalCorpusTest.java:864; ImplementationTableTest.java:125; its ratchets.tsv:1 | `bazel run //spec:update_ratchets` | correct |
| CoreImportsParityTest.java:81 (an assert); DynaFnRegistryTest.java:37; NativeSignatureGeneratorTest.java:33 | `bazel run //:update_generated` | correct |
| ReferenceLaneReport.java:20-21; ReferenceLaneTest.java:25-27 | `bazel run //spec:update_reference_lane`; run `bazel test //spec:reference_lane //spec:update_reference_lane_test` by hand | correct; no lane runs it |
| MinimalCorpusTest.java:49; CorpusVerdictTest.java:13 | `//spec:update_rcorpus_<lane>` | correct |
| spec/BUILD.bazel:154 | `bazel run //spec:update_rcorpus_duckdb` re-blesses a reviewed change | correct |
| NativeMembershipDraft.java:25 | `bazel run //core:draft_native_membership` | correct |
| TokenDump.java:32; scripts/parser/README.md:133; fixtures.py:280; HANDOFF.md:88-89 | `bazel run //tools/engine-runner:update_vocab`, then `//scripts/parser:update_keyword_coverage`, "after a release bump" | correct, but redundant: the bump's `//:update_generated` runs both |
| keywords.py:356; keyword-coverage.tsv:1; HANDOFF.md:27 | `bazel run //scripts/parser:update_keyword_coverage` | correct |
| ReachabilityMetadata.java:32; docs/WAREHOUSE_W1_DESIGN_2026_09_26.md:346 | `bazel run //warehouse:update_reachability_metadata` | correct |
| docs/EXECUTION_PLAN_2026_09_26.md:568, :576 (W1.x plan items) | future HIR/SQL dumps "blessed by `//:update_generated`" | conflicts with the later P2-15/D9 rule that measured goldens are re-blessed deliberately (R:102-104). It is a plan, not machinery |
| spec/corpus.bzl:148 (comment on the rcorpus writer) | "//:update_generated runs it with every other generated file" | **STALE**: removed from the root in `77f4107c1`, contradicted by R:102-104 |

---

## 6. What runs where today

Each list is `deps(<lane targets>) intersect <generators>`, from cquery, with the analysis-only edges removed:

- `//tools/guards:classpath_test` → `classpath_reports` → every package's `guard_classpaths` (`classpath.bzl` writes
  its report from `JavaRuntimeClasspathInfo` with `ctx.actions.write`, and consumes no output);
- `markdown_inputs_test` → `guard_markdown` (`markdown.bzl:9-18`, same pattern);
- `compile_only_test` → `build_action_kinds`, an aspect that writes at analysis time (`compile_only.bzl:21`, :100-101).

Through those edges cquery reaches every generator, including `reference_lane_report` and the `judge_*` passes, but
none of them is executed. Where a test has a generator's output in its runfiles or classpath, the generator executes.

| Where | Generators executed |
|---|---|
| CI lane **1** | `core:stress_index`, `scripts/corpus:gen_differential` |
| CI lane **checks** (gates-run.yml:51) | the 29 of `//:generated` (§3), plus `//tools/java_run:pins` (pins_test) and the file lists of `//core:guardrails` and `//core:census` (`core_test_jar`, `guardrails_sources`, `product_jars`, `census_sources`, `stress_index`) |
| CI lane **3** | `spec:core_main_sources` (a file list) |
| CI lane **4** | `spec:judge_host_duckdb`, `judge_database_duckdb`, and their 5 roster diff tests |
| CI lane **5** | `spec:judge_host_h2`, `judge_database_h2`, and their 5 roster diff tests |
| CI lanes **6**, **7**, **7p**, **9** | `pct:adapter_par` (6 also `pct:discipline_sources`). Gate 9 compares against the committed pct `ratchets.tsv` and never runs `pct:ratchets` |
| CI lane **8** | `parser-equivalence:engine_jars`, `parity_sources` (it reads the committed manifest and fixtures, not the generators) |
| CI lane **10** | `core:stress_index` |
| CI lane **app** | `datacube:cube_jvm_answers`, `cube_queries`, `dist`, `warehouse:duckdb_extensions`, `duckdb_library`, `wasm:jvm_answers`, `zone_jvm` |
| CI lane **misc** | none |
| CI lane **native** | `datacube:dist`, `warehouse:duckdb_extensions`, `duckdb_library` |
| CI lane **browser** (Linux) | `warehouse:duckdb_library` |
| CI lane **build** (`bazel build //...`, gates-run.yml:63, :163) | every **non-manual** generator (55 of the 67 in all.txt), the drafts' generators included (`native_membership_draft`, `gen_own_corpus_draft`, `link_dictionary_next`), the four `judge_*` corpus passes, **plus the manual `//pct:ratchets`** through `//:update_generated` (query `somepath(<non-manual, minus the guard reports>, //pct:ratchets)` → `//:update_generated → //pct:update_ratchets → //pct:ratchets`). The other 11 manual generators are reached only through the analysis-only guard edge, so they are not executed. It writes nothing; it runs no diff test |
| diagnostics.yml | `parser-equivalence:engine_jars`, `parity_sources` |
| **`//gates:local`** (289 tests; computed from the union of `tests(//gates:local)`, because cquery cannot analyse the suite, see §8 P9) | the checks-lane set (all 29 of `//:generated`, `java_run:pins`, the core file lists), plus `scripts/corpus:gen_differential`, `pct:discipline_sources`, `spec:core_main_sources`, `datacube:cube_jvm_answers`, `cube_queries`, `dist`, `warehouse:duckdb_extensions`, `duckdb_library`, `wasm:jvm_answers`, `zone_jvm`. No corpus pass, no `pct:ratchets` |
| **The bump** | phase 2: the 31 generators under `//:update_generated` (§2). Phase 3 (`bazel test //...`): the build lane's set (with `pct:ratchets`), plus running every non-manual test, including both corpus lanes and all 58 non-manual diff tests |
| **By hand** | the drafts (`bazel run //core:draft_native_membership`, `//docs:draft_own_corpus_ledger`, `//datacube:cut_link_dictionary`); `//spec:update_rcorpus_<lane>`; `//spec:update_reference_lane` and its test (ReferenceLaneTest.java:27); `bazel build //spec:native_declarations` (GATES.md:74); `bazel run //core:stress_tool` |

**No CI lane runs `bazel test //...` or any writer.** CI only builds, and checks `//:generated`, `pins_test` and the
roster diff tests.

---

## 7. The diff tests' own messages

Each message is the bazel_lib default text (§0) followed by the package's own suffix. Read from
`bazel query 'kind(_diff_test, //...)' --output=build`, attribute `failure_message`.

| Diff tests | Message (default part → suffix) | Names the right fix? |
|---|---|---|
| `//core:update_generated_<0-5>_test` | `bazel run @@//core:update_generated` / `//core:update_generated_<i>` → "This file is generated from the pinned upstream release — regenerate: bazel run //:update_generated" | It works, but the reason is partly wrong: `native-claims.tsv` and `prelude.pure` also move with core's own code (gen_claims and gen_prelude read `//core:main_java`). After a bump, one run may not converge (§2a), and the message does not say to repeat it. It also names three different commands; the suffix's command regenerates all 48 files, including `pct:ratchets` and the server-backed saved queries |
| `//core:update_ladder_<0-11>_test` | `@@//core:update_ladder` / `_<i>` → "emission moved -- if the shape change is deliberate, re-pin: bazel run //core:update_ladder, and review the diff" | yes |
| `//core:update_stress_corpus_<0-10>_test` | `@@//core:update_stress_corpus` / `_<i>` → "generated by scripts/corpus -- regenerate: bazel run //core:update_stress_corpus" | yes |
| `//datacube:update_generated_<0-3>_test` | `@@//datacube:update_generated` / `_<i>` → "legend-lite changed a fact this cube mirrors … (a changed Pure kind or pivot separator changes what the grid shows; a changed offer fact …)" | The command is right. The reason is **stale**: "Pure kind or pivot separator" is lite-facts' text, and lite-facts moved to engine-client. `test_imports.bzl` (`_3`) is no legend-lite fact at all: it moves with datacube's own TS imports |
| `//docs:update_generated_test` | `bazel run //docs:update_generated` → "generated from the pinned upstream release — regenerate: bazel run //:update_generated" | The command works. The reason is incomplete: `gen_roster` also reads our test sources (`//core:srcs`, `//pct:srcs`, `//spec:srcs`), so an edit to our snippets moves it |
| `//engine-client:update_generated_test` | `bazel run //engine-client:update_generated` → "legend-lite changed a fact … (a changed Pure kind or pivot separator …)" | yes |
| `//fixtures/saved-queries:update_generated_<0-3>_test` | `@@//fixtures/saved-queries:update_generated` / `_<i>` → "A saved-query record changed -- regenerate … then read the diff" | yes |
| `//legend-art:update_generated_test` | `//legend-art:update_generated` → "generated from the pinned react-icons" | yes |
| `//parser-equivalence:update_generated_<0,1>_test` | `@@//parser-equivalence:update_generated` / `_<i>` → "generated from the pinned upstream release — regenerate: bazel run //:update_generated" | It works. The package target is enough; the suffix's command costs the whole repository |
| `//parser-equivalence:update_ratchets_test`, `//spec:update_ratchets_test` | `//…:update_ratchets` → "A measured … ratchet moved -- bazel run …:update_ratchets, and give the reason in the commit" | yes |
| `//pct:update_ratchets_test` (manual) | `//pct:update_ratchets` → "A Channel B discovery count moved -- …" | yes, but the test runs in no lane |
| `//scripts/parser:update_keyword_coverage_test` | `//scripts/parser:update_keyword_coverage` → "the keyword census moved -- …" | yes |
| `//spec:update_rcorpus_<lane>_<0-4>_test` | `@@//spec:update_rcorpus_<lane>` / `_<i>` → "A measured corpus roster moved -- bazel run //spec:update_rcorpus_<lane>, and give the reason … never a roster line to accept silently" | yes |
| `//spec:update_reference_lane_test` (manual) | `//spec:update_reference_lane` → "a deliberate front-end change re-blesses it … with the GATES entry" | yes, but the test runs in no lane |
| `//tools/engine-runner:update_vocab_test` | `//tools/engine-runner:update_vocab` → "the runner's lexer vocabulary at the pinned release" | The command is right. The reason is incomplete: vocab also reruns on any core edit, though its bytes should only move with the runner's jars |
| `//warehouse:update_reachability_metadata_test` | `//warehouse:update_reachability_metadata` → "an FFM signature, or a declared JDK service" | yes |
| `//tools/java_run:pins_test` | no message: only `FAIL: files "…/pins.txt" and "…/pins/pins.expected" differ.` | It names no fix. The fix is a hand edit of `pins.expected` (tools/java_run/BUILD.bazel:8-9) |

On Windows the message prints, but the diff itself does not: `fc.exe` output goes to NUL on a plain difference, and
the message names a `diff` command with runfiles paths (`diff_test_tmpl.bat:84-106`).

---

## 8. Problems

- **P1. A manual target is pulled into the build.** `//:update_generated` is neither manual nor tagged
  (R:105-125), and it lists the manual `//pct:update_ratchets`. So `bazel build //...` (CI's build lane),
  `bazel test //...` (bump phase 3, README) and every `bazel run //:update_generated` run the manual 2 GB
  `//pct:ratchets` (with its 4 GB PAR). Evidence: `somepath`, §2 and §6. The design's R4 already says so.
- **P2. One update run is not a fixed point** (§2a). Five generators (`gen_natives`, `gen_dynafn`,
  `gen_engine_handlers`, `gen_prelude` and `spec:ratchets`), plus every generator built on core, read other
  generators' committed copies, compiled or as classpath resources. `gen_natives` and `gen_prelude` form a cycle
  through committed files. The bump runs the update once and blames what is left on "the judgement half"
  (Bump.java:154-156).
- **P3. Self-inputs.** An update invalidates its own generators, and so does the corpus (§2b).
  - `ladder_report`, `spec:ratchets`, `pct:ratchets`, `gen_claims`, four parser-equivalence generators and
    `reachability_metadata` take their own committed outputs as inputs.
  - `gen_roster` and `parser-equivalence:ratchets` take every rcorpus roster, the reference golden and both other
    ratchet files.
  - The corpus passes take `spec ratchets.tsv`, the ladder pins and the reference golden.
  - So after `bazel run //:update_generated`, the next build reruns these, and a moved spec ratchet or ladder pin
    reruns both corpus lanes.
- **P4. Writers that re-bless a test's expected results.** `//:update_generated` rewrites, with no separate decision:
  - the ladder emission pins (`//core:update_ladder`). The design (§4.2 E) says they should be "re-pinned only
    explicitly, never by `//:update_generated`";
  - the three measured ratchet files (`spec`, `parser-equivalence`, `pct`), which D9 (b) allows;
  - `docs/protocol-roster.tsv`, a ledger gate 8 reads (`_LEDGERS`, parser-equivalence/BUILD.bazel:142-152), whose
    header says "a bump that … moves a tag … is a reviewed diff".

  Because the bump writes them first, phase 3 never reports these moves. The rcorpus rosters are the only measured
  expectations held out (R:102-104).
- **P5. Writers and diff tests outside any root target, and nothing to catch a new one.**
  - `//pct:update_ratchets_test` and `//spec:update_reference_lane_test` run nowhere.
  - The reference-lane golden is checked by no lane and no bump.
  - `//pct:update_ratchets` is written by the root writer but checked by no root suite.
  - No guard ties suites to `//:generated` or writers to `//:update_generated`. The claim at R:78 covers only
    deletion (§1b).
- **P6. Two suites in neither root target:** `//spec:update_rcorpus_duckdb_tests` and `_h2_tests`. This is by design;
  they run in lanes 4 and 5.
- **P7. Vocab is built twice, and core compiled in exec, under `//:generated`.** The keywords tool's `data` carries
  `//tools/engine-runner:vocab` into `keyword_coverage`'s action as runfiles. That builds a second vocab and a second,
  exec-config core: 40 exec Javac actions, which the checks lane, `//gates:local` and every update pay. It also makes
  `keyword_coverage` rerun on every core edit. This contradicts commit `b245f5f30` (§2).
- **P8. Stale instructions** (§5):
  - GATES.md:39-47, which names `//query:update_generated_test`;
  - datacube/src/grid/columns.ts:46;
  - TypeFacts.java:26-27;
  - scripts/corpus/dense_build.py:10;
  - the "`//core:update_ladder_test`" name in core/BUILD.bazel:791, LadderRender.java:30 and LeanSqlLadderTest.java:23;
  - spec/corpus.bzl:148;
  - release.MODULE.bazel:20 → UPSTREAM_BOUNDARY_HOMEWORK §5 (Maven commands, deleted scripts);
  - datacube's diff-test reason text;
  - README.md:294 and R:99-101 ("every generated file … from the pinned upstream release").
- **P9. cquery cannot analyse `//gates:local`.** Running `bazel cquery //gates:local` fails with 40 "Visibility error"
  lines: `//:generated`, `//spec:spec_tests`, the guard tests and others are private to their packages. `bazel test`
  and `bazel build //...` expand test suites before analysis, so this probably only blocks tooling. **OPEN**: settle
  with `bazel build --nobuild //gates:local`, which I was not allowed to run.
- **P10. Two design-doc facts are wrong:**
  - R2 says gen_dynafn uses "no core class at all". It reads `EngineHandlers` and `Pure` (`DynaFnGenerator.java:97-106`);
  - R4 says `//:update_generated` "builds every generator a second time". It shares their artifacts; only vocab and the
    exec-config core are doubled (aquery, §2).
- **P11. The drafts are not manual.** `bazel build //...` runs `native_membership_draft` and `gen_own_corpus_draft`
  (which reads upstream trees and `pe_tests_lib`) on every build. `link_dictionary_next` is built on purpose
  (datacube/BUILD.bazel:303-304).

## Open questions

- Does `gen_natives`' read of the committed prelude (through NameResolver's static initialiser) change `Pure.java`?
  Does `gen_claims`' classpath reach the committed `engine-handlers.tsv` through a static initialiser? **Settle:** run
  `//spec:gen_natives` and `//spec:gen_claims` with a perturbed committed prelude or tsv, or add a
  `-verbose:class`/resource trace to the action.
- How many `//:update_generated` passes does a real bump need? **Settle:** after a bump that moves Pure.java, run it
  until `git status` is clean (design §7), and record the count.
- Does `bazel build //...` analyse `//gates:local`, given the visibility errors? **Settle:**
  `bazel build --nobuild //gates:local //...`.
- Is every generator in `//:generated` byte-identical on Linux, macOS and Windows? The checks lane requires it on all
  three. **Settle:** read a recent green CI run of the checks lane on each platform.
