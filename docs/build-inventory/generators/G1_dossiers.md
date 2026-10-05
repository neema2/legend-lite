# G1 dossiers: the spec generator chain and the spec measurements

Repo: `the build/rebuild checkout` (branch build/rebuild, HEAD `669b39ad1`). Paths are
repo-relative. `G/` = `spec/src/gen/java/com/legend/generators/`, `CL/` = `spec/src/gen/java/com/legend/claims/`,
`T/` = `spec/src/test/java/com/legend/`, `C/` = `core/src/main/java/com/legend/`, `RES/` =
`core/src/main/resources/com/legend/builtin/`.

"aq" = `env -C <repo> bazel aquery 'mnemonic("Generate|Measure", //spec:gen_natives + ... 12 labels)' --output=jsonproto`,
summarized per action by input category (script in the session scratchpad; run 2026-10-05). Counts: `@legend_engine_src//:tree`
is 12,900 files (3,253 `.pure`), `@legend_pure_src//:tree` is 2,693 files (281 `.pure`) (`find -L` over the output base's
external dirs). The three engine "spec roots" (`G/PreludeGenerator.java:139-145`) hold 1,189 + 790 + 5 = 1,984 `.pure`.
Every java_run action also carries 118 JDK files (the remote JDK 25) and runs `java -cp <jars> <main> <args>` with
`-Duser.timezone=GMT -Duser.language=en -Duser.country=US -Dfile.encoding=UTF-8 -Djava.io.tmpdir=<out>_tmp` (aq).
`java_run` sets `-Xmx` and a scheduler resource_set only when `memory_mb` is given (`tools/java_run/defs.bzl:92-114`); none of
the eight chain/draft generators below sets it, so each runs at the JVM's default heap with no resource_set.

The four measurement dossiers (ratchets, reference_lane_report with ref_dump, eager_corpus_compile, eager_corpus_compile_world2)
follow the chain section.

---

## Part 0. The chain as a whole

### 0.1 The links and their wiring

```
upstream trees ─┬─> gen_natives  ──(generated/Pure.java)──┬──> gen_prelude ──(prelude.pure)──> core_next_prelude ─┐
                │      ^ committed Pure.java (splice),    │        ^ core/src/main/java as text                   │
                │        native-membership.tsv            │                                                       v
                ├─> gen_dynafn ──(generated/DynaFn.java)──┼────────────────────────────> core_next ──> claims_generator_lib ──> gen_claims
                │      ^ committed DynaFn.java (splice)   │                                ^ core/src/main/java as text ─────────┘
CompileContext ─┴─> gen_imports ─(generated/NameResolver.java)┘
Handlers.java ────> gen_engine_handlers (no chain edge: reads committed core only)
```

Each arrow labelled with a file is a declared build edge (`spec/BUILD.bazel:316-512`, `core/BUILD.bazel:843-873`). What the
diagram cannot show is the second set of edges: EVERY generator except gen_claims also runs on `//core` AS COMMITTED (its
library `:generators` deps `//core`, `spec/BUILD.bazel:55-68`), so it calls compiled classes and reads resources built from
the committed copies of the chain's own outputs. Those reads are listed per link below and drive the fixed-point answer (0.4).

| link | splices (rewrites part of) | upstream read | our code: COMPILED (committed //core) | our code: TEXT | chain build outputs read |
|---|---|---|---|---|---|
| gen_natives | `C/builtin/Pure.java`: the text inside each membership constant's `signature("…")`, missing constants appended, and the `AT_*` overload-group block regenerated (`G/NativesGenerator.java:63-64,92-156,195-225`) | every `.pure` under the 3 engine spec roots and the whole pure tree (regex index), then a full parse of each file that declares a membership FQN; `m3.pure` (`:277-311`) | `Compiler.parseSources`, `NameResolver.resolve` (uses committed `CORE_IMPORTS`; class init loads committed `Pure` and `prelude.pure`, `C/compiler/NameResolver.java:296-345`), `ElementParser.parse` (`:319-328,417-424`) | `RES/native-membership.tsv`, committed `Pure.java` | none |
| gen_imports | `C/compiler/NameResolver.java`: the list between `CORE_IMPORTS = List.of(` and `);` (`G/ImportsGenerator.java:31,62-74`) | `CompileContext.java` text, the `META_IMPORTS` literal (`:47-59`) | none (imports no core class) | committed `NameResolver.java` | none |
| gen_dynafn | `C/builtin/DynaFn.java`: the enum member block (`G/DynaFnGenerator.java:123-156`) | every `.pure` in the WHOLE engine tree (`Files.walk(engineRoot)`, `:66-90`); the 24 files that matter are all under `legend-engine-xts-relationalStore` (grep of the archive) | `EngineHandlers.fqnsOf` (reads committed `RES/engine-handlers.tsv`, `C/builtin/EngineHandlers.java:73,84`) and `Pure.SQL_NULL/SQL_TRUE/SQL_FALSE` (`:96-119`) | committed `DynaFn.java` (keeps each member's hand-owned Resolution and Lite constant, `:124-138`) | none |
| gen_engine_handlers | none (whole file) | `Handlers.java` text (`G/EngineHandlersGenerator.java:53,65-78`) | `Pure.all()`, `Pure.LITE_SURFACE`, `Pure.Lite.PKG`, `Prelude.elements()` (reads committed `RES/prelude.pure`, `C/builtin/Prelude.java:42`), `SignatureMangle.mangle` (`:80-126`) | none | none |
| gen_prelude | none (whole file) | 3 engine spec roots + whole pure tree (index), the relational corpus dir, `UpstreamFiles.LIBRARY_FILES/SHAPE_FILES/PLATFORM_ROOTS`, `m3.pure` (`G/PreludeGenerator.java:160-267,389-405,1364-1400`) | `Pure.nativeFunctionsAt` (`:436,556,583`), `SystemMetamodel.source()/elementFqns()` (`:272,363,385,435,544,578`), `CoreFn.of` (`:438,558,567,585`), `NameResolver.CORE_IMPORTS` (`:321,475`), `Claims.claimedBareNames()` (`:429,541`; `:claims` is compiled against `//core`, `spec/BUILD.bazel:74-86`), our parser | every `.java` under `core/src/main/java` except `Prelude.java` (FQN-token demand scan, `:338-357`), `Pure.java` hand `native Class`/`Enum` text (`:1600-1612`) | `Pure.java` = gen_natives' output (override `com/legend/builtin/Pure.java=…`, `spec/BUILD.bazel:462`); DynaFn.java and NameResolver.java are the COMMITTED copies |
| gen_claims | none (whole file) | none | runs on `//core:core_next`: `Pure` (constants and `AT_*` groups by reflection), `Claims` over `RegistryKeys`, `CoreFn`, `Pure.walledNativeFqns`, `NativeFn.families` (`CL/Claims.java:91-128`, `CL/ClaimsGenerator.java:58-87`) | every `.java` under `core/src/main/java` (the `also` column, `CL/ClaimsGenerator.java:93-154`) | DynaFn.java, Pure.java, NameResolver.java = the three Java outputs, both compiled (core_next) and as text overrides (`spec/BUILD.bazel:503-505`); prelude = gen_prelude via core_next_prelude |

### 0.2 core_next and core_next_prelude

- `//core:core_next` (`core/BUILD.bazel:843-867`): a legend_java_library (NullAway on) of every `src/main/java/**/*.java`
  except DynaFn.java, Pure.java, NameResolver.java, plus `//spec:gen_dynafn`, `:gen_imports`, `:gen_natives`; resources = all
  of `src/main/resources/**` EXCEPT prelude.pure (so the COMMITTED engine-handlers.tsv, native-claims.tsv and
  native-membership.tsv ride in it: `bazel query 'somepath(//spec:gen_claims, //core:src/main/resources/com/legend/builtin/native-claims.tsv)'`
  → `gen_claims claims_generator_lib core_next native-claims.tsv`); `runtime_deps = [":core_next_prelude"]`.
- `//core:core_next_prelude` (`core/BUILD.bazel:869-873`): a resource-only library holding `//spec:gen_prelude`'s output with
  `resource_strip_prefix = "spec/generated"`.
- Purpose, stated at `core/BUILD.bazel:838-842`: "the one generator that reads the COMPILED registries the others rewrite.
  At a fixpoint (every committed file current) it is core, byte for byte."
- Only consumer: `bazel query 'rdeps(//..., //core:core_next, 1)'` → `//spec:claims_generator_lib` only; and that library's
  only rdep is `//spec:gen_claims`. So core_next exists solely for native-claims.tsv.
- Cost: a second full compile of all 751 core main `.java` files (748 committed + 3 generated) (aq for gen_claims shows `libcore_next.jar` and `libcore_next_prelude.jar` as
  its only core jars), rerun on every core main-source edit.
- Introduced: `998f1a41c` (2026-09-23, "Bazel replaces Maven", #7) with the rest of the Bazel generator wiring.

### 0.3 The writers //core:update_generated_0..5 and their diff tests

`write_source_files(name = "update_generated", files = _GENERATED, …)` (`core/BUILD.bazel:699-706, 782-787`) expands into one
`_write_source_file` per entry, in dict order, each with a `_diff_test` (query: `bazel query 'attr(name, "update_generated", //core:*)' --output=label_kind`;
mapping from `labels(in_file, //core:update_generated_N)`):

| writer | writes | from | diff test |
|---|---|---|---|
| `//core:update_generated_0` | `core/src/main/java/com/legend/builtin/DynaFn.java` | `//spec:gen_dynafn` | `//core:update_generated_0_test` |
| `//core:update_generated_1` | `core/src/main/java/com/legend/builtin/Pure.java` | `//spec:gen_natives` | `_1_test` |
| `//core:update_generated_2` | `core/src/main/java/com/legend/compiler/NameResolver.java` | `//spec:gen_imports` | `_2_test` |
| `//core:update_generated_3` | `core/src/main/resources/com/legend/builtin/engine-handlers.tsv` | `//spec:gen_engine_handlers` | `_3_test` |
| `//core:update_generated_4` | `core/src/main/resources/com/legend/builtin/native-claims.tsv` | `//spec:gen_claims` | `_4_test` |
| `//core:update_generated_5` | `core/src/main/resources/com/legend/builtin/prelude.pure` | `//spec:gen_prelude` | `_5_test` |

- Failure message: "This file is generated from the pinned upstream release — regenerate: bazel run //:update_generated"
  (`core/BUILD.bazel:784`). It is wrong for five of the six: their outputs also move on our own edits (see each dossier).
- The six diff tests form `//core:update_generated_tests`, listed in `//:generated` (`BUILD.bazel:82`). `//:generated` is in
  CI's `checks` lane (`.github/workflows/gates-run.yml:51`) on linux, macos and windows (`.github/workflows/gate.yml:66-90`),
  and in `//gates:local` (`gates/BUILD.bazel`, first entry). None is manual (`bazel query 'attr(tags, manual, …)'`).
- `//core:update_generated` is in `//:update_generated`'s `additional_update_targets` (`BUILD.bazel:109`; the root writer is `testonly`, `:106-107`). The root writer is
  `testonly` and NOT manual, so `bazel build //...` builds it (and every generator under it).
- The bump runs `bazel run //:update_generated` ONCE (`tools/bump/Bump.java:148-151`), then `bazel test //...`
  (`:153-156`), which includes these six diff tests.
- Each splicing generator declares its own committed file as an input (`spec/BUILD.bazel:335,353,374`), and every
  generator reads the committed generated files through `//core`. So the committed copies are inputs to the chain, not
  only outputs.

### 0.4 Does one `bazel run //:update_generated` reach a fixed point? No, not in general (settled from the code)

A run computes every output from the COMMITTED tree. Only gen_prelude's Pure.java text, gen_claims' compiled core
(core_next) and gen_claims' text overrides see this run's outputs. The other links read the committed copies of their
upstream links, through compiled `//core` or as declared text.

- **Committed Pure.java (compiled):** read by gen_engine_handlers (`Pure.all()` mangles become the `id → fqn` join,
  `G/EngineHandlersGenerator.java:81-84,100-105`) and gen_prelude (`Pure.nativeFunctionsAt`, `:436,556,583`). gen_natives
  loads it too, through `NameResolver`'s class init.
- **Committed prelude.pure (resource):** read by gen_engine_handlers (`Prelude.elements()` function ids, `:85-89`). gen_natives
  and gen_prelude also load it in `NameResolver`'s static initializers (`C/compiler/NameResolver.java:296-345`); gen_natives
  resolves with `preludeOn = false` (`:247-249`), so whether it reaches their output is OPEN.
- **Committed NameResolver.CORE_IMPORTS (compiled):** read by gen_natives (resolution, `NameResolver.java:376,713`) and
  gen_prelude (`G/PreludeGenerator.java:321,475`, plus resolution).
- **Committed engine-handlers.tsv (resource):** read by gen_dynafn (`EngineHandlers.fqnsOf`, `G/DynaFnGenerator.java:105-119`).
- **Committed DynaFn.java and NameResolver.java (text):** read by gen_prelude's demand scan. The `com/legend/builtin/Pure.java`
  override is the only one it gets (`spec/BUILD.bazel:462`).
- **Committed core's Claims (compiled):** gen_prelude's `:claims` is compiled against `//core`, not core_next
  (`spec/BUILD.bazel:74-86`).

Consequences, by what the bump moves (each step is one more run before the diff tests are green):
- **A respelled signature in Pure.java:** run 1 writes the new Pure.java, but engine-handlers.tsv is computed from the old
  compiled Pure. A changed id can fill or empty a row's fqn. Run 2 rewrites engine-handlers.tsv. Run 3 can then change
  DynaFn.java, because a PURE member's FQN list comes from `EngineHandlers.fqnsOf` over the committed table. In the same run,
  native-claims.tsv's `also` column can change, because it scans DynaFn.java's text for quoted FQNs
  (`CL/ClaimsGenerator.java:145`).
- **A moved prelude.pure:** run 1's engine-handlers.tsv used the old prelude's function ids, so run 2 differs, then run 3 as
  above.
- **A changed Handlers.java:** run 1's engine-handlers.tsv is right. Run 1's DynaFn.java used the old table, so run 2 differs,
  and native-claims.tsv follows in run 2.
- **A changed META_IMPORTS:** NameResolver.java is right in run 1, but gen_natives and gen_prelude resolved with the old
  CORE_IMPORTS. Run 2 can change Pure.java and prelude.pure, run 3 engine-handlers.tsv, run 4 DynaFn.java and
  native-claims.tsv.
- **A hand edit to native-membership.tsv that adds a constant without hand-adding it to Pure.java** (gen_natives' `addMissing`
  path, `G/NativesGenerator.java:138-153`): run 1's engine-handlers.tsv and prelude.pure use the old compiled Pure, so run 2 is
  needed. Commit `1446b82c0` (2026-10-04) avoided this by adding the six Pure.java constants by hand and then running
  `//:update_generated` (commit message).

What exists: the diff tests catch a missed run, and their message says to run `//:update_generated` again. The bump does
not loop and documents no second run (`Bump.java:40-44,148-156`; its message calls the step idempotent, which holds only at
the fixed point). The design doc's open item (`BUILD_REBUILD_DESIGN_2026_10_05.md:555-556`) and area 2's "doubtful (OPEN)"
(`area2_report.md:59-63`) are settled by the reads above: one run does not guarantee a fixed point, and the dependency depth
through committed copies is up to 4 runs (imports → natives/prelude → handlers → dynafn/claims).

What would close it with one run (no code change to the logic, only wiring):
- give gen_engine_handlers gen_natives' and gen_prelude's outputs, compiled together with core (a core_next-like classpath,
  or a small extraction that hands it the catalog and the prelude's function ids);
- give gen_dynafn gen_engine_handlers' output in place of the committed table;
- give gen_natives and gen_prelude the generated CORE_IMPORTS;
- give gen_prelude the generated DynaFn.java and NameResolver.java as overrides, and Claims of the regenerated core.

The order is then a DAG: imports → natives → prelude → handlers → dynafn → claims.

### 0.5 native-claims.tsv: every reader, and what retiring it loses (for D2)

Search: `git grep -n "native-claims"` and `git grep -n "claims\.tsv"` over the whole repo, plus `CoreTree.resource` users.
- **Product (core and every server or app):** none. `//core:builtin` excludes it from its resources (`core/BUILD.bazel:87-92`:
  "READ only by the spec module's claim tests: it is not product data (retiring at step 5)"). Neither wasm nor sdlc-server
  embeds it: they embed only prelude.pure and engine-handlers.tsv (`wasm/src/main/java/planner/PreludeResources.java:22`,
  `sdlc-server/src/main/java/com/legend/sdlc/page/PageResources.java:15`).
- **Tests:**
  - `//core:update_generated_4_test` (the diff test) is the ONLY code that opens the file.
  - `T/claims/ClaimRegistryTest.java:50` declares `RESOURCE = CoreTree.resource(".../native-claims.tsv")` and never uses it
    (no other occurrence in the file). Its one test computes `Claims.all()` live and holds `UNCLAIMED_MAX = 0` (`:84-117`).
  - `core/src/test/java/com/legend/builtin/NativeFunctionTest.java:54` mentions it in a comment only.
  - So the design doc's "its one test" (D2, `BUILD_REBUILD_DESIGN_2026_10_05.md:202,490`) and area 1's "its readers are
    spec's ClaimRegistryTest" (`area1_report.md:184-186`) are wrong: no test reads the file as data.
- **Generators:**
  - PreludeGenerator uses `Claims.claimedBareNames()` (compiled), not the file (`G/PreludeGenerator.java:429,541`); its only
    mention is the text of a comment it prints into prelude.pure (`:729`, which lands at `RES/prelude.pure:6600`).
  - `docs/plan-audit-2026-09-26/code-traps.md:161` ("consumed by PreludeGenerator") is therefore wrong.
- **As a declared input but never read:** `//core:core_next` resources (it rides in core_next's jar), `//core:srcs` and
  `//core:main_srcs` (spec_tests' data).
- **Docs and humans:**
  - `docs/GATES.md:44` (a generated file under //:generated) and GATES history entries `:420,426,4174,4628,5234,5636,6759`;
    `docs/NATIVE_PROVENANCE_2026_09_11.md:3,1072` (cites claim kinds from it).
  - `docs/UPSTREAM_BOUNDARY_PROGRAM.md` §3 D says it retires at untangle step 5, and so does the file's own header line
    (`CL/ClaimsGenerator.java:209-210`).
  - Bump.java's NEXT list says to read the diff of `native-*.tsv` (`tools/bump/Bump.java:163`).
- **Churn:** 10 commits touched it since 2026-09-23 (`git log --since=2026-09-23 -- RES/native-claims.tsv`). In every one it
  was regenerated beside a code change, never an upstream bump.

**Retiring it loses:**
1. The reviewed diff of the implemented surface: which owner claims each overload, its kinds, its constant names and the
   `also` column (the files that name the overload). `ClaimRegistryTest`'s javadoc calls that column "Evidence for a reviewer,
   not a claim" (`:42-46`).
2. The forcing function: today any core edit that adds or removes a `Pure.X` reference or a quoted FQN in main Java turns
   `//:generated` red until the ledger is regenerated.

**Retiring it does not lose:**
- the UNCLAIMED ratchet (live in `ClaimRegistryTest`);
- the prelude's exclusion rule (live `Claims` in PreludeGenerator);
- any product behaviour.

**What it removes:**
- `//core:core_next`, `//core:core_next_prelude`, `//spec:claims_generator_lib` and `//spec:gen_claims`;
- `//core:update_generated_4(_test)`;
- the 751-file second compile on every core edit;
- core_next's edges to its own committed output.

The middle option, keeping it as a build output, also deletes core_next: build it on demand against committed `//core`
through the existing `//spec:claims` library, which already holds both classes (`spec/BUILD.bazel:74-86`). The source tree
then needs no overrides, because the committed sources ARE the tree.

---

## //spec:gen_natives

1. **Identity.** `_java_run` (macro `java_run`, `tools/java_run/defs.bzl:122`), `spec/BUILD.bazel:370-389`, not manual.
   Program `com.legend.generators.NativesGenerator`, entry `G/NativesGenerator.java:47`. Writer `//core:update_generated_1`.
2. **What it computes.** Pure.java declares one Java constant per platform native (`… = signature("native function …")`).
   WHICH natives exist is ours (native-membership.tsv: constant, FQN, signature key). HOW each is spelled is upstream's. The
   program finds upstream's declaration of each membership row in the pinned trees, renders it canonically (FQN-qualified),
   writes that text into the matching constant, appends missing constants, and regenerates the `AT_*` overload-group lists.
   It REFUSES (fails the action) on a membership row with no upstream partner (`DIVERGENT_MAX = 0`, `:72,229-240`), on
   unresolved upstream type names (`:380-383`), and on an overload declared twice with different text (`:372-376`).
3. **Why it exists.** Upstream boundary batch 5 (D4): `d4ff27c2a` (2026-09-11) "Pure.java's signature text is GENERATED from
   the checkouts" (then a test's write mode). It became a program and a Bazel action in `998f1a41c` (2026-09-23, #7). It
   protects against hand-typed signatures drifting from upstream's. The reason is still live.
4. **Inputs, declared** (aq: 15,749):
   - upstream: engine tree 12,900 + pure tree 2,693 (`UPSTREAM_TREES`);
   - our sources: `RES/native-membership.tsv`, committed `C/builtin/Pure.java` (2);
   - jars: 31 core layer jars, plus base, json, `spec:claims`, `spec:generators`, `spec:source_tree` (5);
   - other generators' outputs: none;
   - JDK: 118 files.
5. **Inputs, actually read.**
   - **Files:** `.pure` under the 3 engine spec roots (1,984) and the whole pure tree (281), each read and regex-scanned;
     every file declaring a membership FQN is parsed whole (`:277-308,317-322`); `m3.pure` (`:309-311`); the membership TSV;
     Pure.java.
   - **System property:** `natives.debug` (optional, `:329`).
   - **Compiled committed core:** the parser, `NameResolver` (its committed CORE_IMPORTS; its class init loads committed
     `Pure` and `RES/prelude.pure`), `ElementParser`.
   - **Over-declared:**
     - about 10,900 engine files outside the spec roots, and every non-`.pure` file of both trees;
     - `libclaims.jar`: NativesGenerator never touches Claims (`somepath(//spec:gen_natives, //spec:src/gen/java/com/legend/claims/Claims.java)`
       → via `:generators` → `:claims`);
     - most of the 31 core jars (e.g. `server_lib`: `somepath(//spec:gen_natives, //core:src/main/java/com/legend/server/DiagramService.java)`
       → `generators core server_lib`). OPEN which jars actually load: settle with one run under `-verbose:class`.
   - **Under-declared:** none (the classpath carries the committed resources it loads).
6. **Outputs.** `bazel-bin/spec/generated/Pure.java`: committed Pure.java with each membership constant's signature text
   replaced, missing constants appended, and the `AT_*` group block regenerated. Stdout gets the divergence receipt.
7. **Committed?** Yes: `core/src/main/java/com/legend/builtin/Pure.java`; writer `//core:update_generated_1`; diff test
   `//core:update_generated_1_test`; in `//core:update_generated_tests` → `//:generated`. Writer in `//core:update_generated`
   → `//:update_generated`.
8. **Who consumes it.**
   - Build: `//spec:gen_prelude` (the Pure.java override), `//core:core_next` (`bazel query 'rdeps(//..., //spec:gen_natives, 1)'`).
   - Product: the committed file is core's catalog (every core consumer).
   - Tests: `T/generators/NativeSignatureGeneratorTest.java` (membership against the compiled constants, `:47-66`).
   - Humans: Bump.java's NEXT list (`:163`), GATES.md:42-47.
9. **Determinism.** Yes:
   - sorted walks (`:294`), insertion-ordered sets, a TreeMap for the groups (`:197`);
   - the output joins lines with `\n` after `readAllLines` (`:54,57`), so it is LF on every platform;
   - no time, randomness or absolute path in the output.
10. **Cost.** One JVM at the default heap. It reads about 2,265 `.pure` files and parses the subset declaring a membership
    FQN (105 engine files carry a native, per `G/PreludeGenerator.java:1381-1382`). No database or server.
11. **What reruns it today.**
    - any upstream file of either archive;
    - native-membership.tsv and Pure.java;
    - ANY core main source or resource, base or json (via `//core`), e.g. the server path above;
    - any `spec/src/gen/java` file, including Claims.java (path above).
12. **True trigger.**
    - an upstream bump;
    - hand edits to native-membership.tsv or to Pure.java's hand parts (constant names, Lite section, hand classes);
    - changes to our parser or resolver, which could change how upstream parses (real but rare; design doc group B).
13. **Who runs it today.**
    - `bazel build //...` (CI build lane, `gates-run.yml:63`);
    - CI checks lane and `//gates:local`, through `//:generated` → `_1_test`;
    - the bump (`bazel run //:update_generated`, then `bazel test //...`);
    - by hand: `bazel run //:update_generated` (README.md:294, GATES.md:47).
14. **Recommendation: COMMITTED-UPSTREAM.** Its spelling comes from upstream; the hand-owned membership and Pure.java's hand
    parts are its own spliced inputs.
    - **Manual?** No while its diff test runs everyday. Once narrowed, the diff test is the trigger.
    - **Writer:** in the chain's update group. Today that is `//core:update_generated`, which `//:update_generated` and the bump
      both run. It cannot be bump-only, because membership edits are everyday work (`1446b82c0`).
    - **Diff test:** in the everyday gate, because Pure.java and membership change in ordinary commits (7 Pure.java commits
      since 2026-09-23).
    - **Narrowing:**
      - its own library deps only the parser, model, protocol and resolver slices (not `//core`), and drops `:claims`;
      - declare the 3 engine spec roots and the pure tree, not the whole engine tree;
      - read the generated CORE_IMPORTS (0.4).
    - D3 (parse with upstream's parser) would remove the parser trigger.
15. **Open questions.**
    - Which core jars load: settle with `-verbose:class`.
    - Whether NameResolver's class-init load of the committed prelude can change its output (`preludeOn = false`): settle by
      reading `resolveNameMulti` for any use of `PLATFORM_FQNS` when `preludeOn` is false.

## //spec:gen_imports

1. **Identity.** `_java_run`, `spec/BUILD.bazel:331-347`, not manual. Program `com.legend.generators.ImportsGenerator`, entry
   `G/ImportsGenerator.java:35`. Writer `//core:update_generated_2`.
2. **What it computes.** Copies legend-engine's implicit import list (`CompileContext.META_IMPORTS`, in order, because
   first-match makes order semantic) into `NameResolver.java`'s `CORE_IMPORTS = List.of(…)`, leaving the rest of that
   hand-written file untouched.
3. **Why it exists.** `b7504b407` (2026-09-11) "CORE_IMPORTS as the engine's sequence"; a program since `998f1a41c` (#7).
   It keeps our resolver's implicit imports equal to the engine's. The reason is still live.
4. **Inputs, declared** (aq: 156):
   - upstream: 1 file (`CompileContext.java`, `spec/BUILD.bazel:300`);
   - our sources: committed `C/compiler/NameResolver.java` (1);
   - jars: 31 core jars plus base, json, claims, generators, source_tree (5);
   - JDK: 118 files.
5. **Inputs, actually read.**
   - **Files:** the two files' text (`:40-41`). No system property or environment.
   - **Over-declared:** all 31 core jars and base, json, claims and source_tree. ImportsGenerator imports no core class
     (`:6-13`). Proof that core reaches it: `somepath(//spec:gen_imports, //core:src/main/java/com/legend/StatementExecutor.java)`
     → `generators core driver`, and `somepath(//spec:gen_imports, //core:src/main/resources/com/legend/builtin/prelude.pure)`
     → `generators core builtin`.
   - **Under-declared:** none.
6. **Outputs.** `bazel-bin/spec/generated/NameResolver.java`: the committed file with the CORE_IMPORTS list rewritten.
7. **Committed?** Yes: `core/src/main/java/com/legend/compiler/NameResolver.java`; writer `//core:update_generated_2`; diff
   test `_2_test` → `//:generated`; in `//:update_generated`.
8. **Who consumes it.**
   - Build: `//core:core_next`, `//spec:gen_claims` (override).
   - Product: the committed file is core's resolver.
   - Tests: `T/generators/CoreImportsParityTest.java:73-87` (compiled CORE_IMPORTS == META_IMPORTS; pure's coreImport ==
     that minus the engine's three), which overlaps with `_2_test`.
   - Docs: `C/compiler/NameResolver.java:206-212`.
9. **Determinism.** Yes: string splice, no ordering hazard. Line endings are the input's own; CI's checkout guards keep LF
   (`.github/workflows/gate.yml:14`).
10. **Cost.** Trivial: two files in one JVM. Its only cost is rerunning on every core edit.
11. **What reruns it today.** Every core main source or resource edit, base, json, any `spec/src/gen/java` file, CompileContext.java,
    NameResolver.java.
12. **True trigger.** An upstream bump (CompileContext.java), plus edits to NameResolver.java (its own spliced file:
    10 commits since 2026-09-23).
13. **Who runs it today.** The build lane, the checks lane, `//gates:local`, the bump, and by hand (README.md:294).
14. **Recommendation: COMMITTED-UPSTREAM.**
    - **Manual?** No.
    - **Writer:** in the chain's update group. `//:update_generated` and the bump both need it.
    - **Diff test:** everyday gate. With the narrowing below it reruns only when NameResolver.java or the pin moves, which
      is honest and cheap.
    - **Narrowing:** its own library with no `//core` dep (it needs none), and drop `:claims` and `:source_tree`.
15. **Open questions.** Whether `CoreImportsParityTest` should go once `_2_test` is the check: settle by deciding whether the
    pure-coreImport subset assertion (`:85`) is still wanted. That half is not covered by the diff test.

## //spec:gen_dynafn

1. **Identity.** `_java_run`, `spec/BUILD.bazel:350-366`, not manual. Program `com.legend.generators.DynaFnGenerator`, entry
   `G/DynaFnGenerator.java:39`. Writer `//core:update_generated_0`.
2. **What it computes.** legend-engine registers relational "dynafunctions" through `dynaFnToSql('name', …)` in its dialect
   extensions, plus a type-inference map. This rewrites DynaFn.java's enum members, one per engine name. Each member gets its
   registering dialects (from the file path) and whether inference knows it. A member keeps its hand-assigned Resolution and
   Lite constant; a new name lands UNSUPPORTED. For PURE members the FQN list is derived from the engine surface.
3. **Why it exists.** `a0b2ea381` (2026-09-11) "the dynafunction registry (DynaFn) verified against the engine checkout"; a
   program since `998f1a41c`. The PURE-FQN column by engine surface came in `c5caddd3b` (2026-09-25, per its subject). The
   reason is still live.
4. **Inputs, declared** (aq: 15,748):
   - upstream: both trees (12,900 + 2,693);
   - our sources: committed `C/builtin/DynaFn.java` (1);
   - jars: 31 core jars plus 5;
   - JDK: 118 files.
5. **Inputs, actually read.**
   - **Files:** every `.pure` file in the whole engine tree (3,253; `:66-70`), read fully, and DynaFn.java.
   - **Compiled committed core:** `EngineHandlers.fqnsOf`, which reads the COMMITTED engine-handlers.tsv
     (`C/builtin/EngineHandlers.java:73`), and `Pure.SQL_NULL/TRUE/FALSE.qualifiedName()` (`:96-99`).
   - **Over-declared:**
     - the whole pure tree (2,693 files; never read: `somepath(//spec:gen_dynafn, @legend_pure_src//:pom.xml)` is direct);
     - every engine file outside `legend-engine-xts-relationalStore` (all 24 registry files live there; grep of the archive);
     - the core jars beyond builtin and model;
     - claims and source_tree.
   - **Under-declared:** none. But the committed engine-handlers.tsv it depends on arrives through `//core`, not as the chain's
     edge from `:gen_engine_handlers`: `somepath(//spec:gen_dynafn, //core:src/main/resources/com/legend/builtin/engine-handlers.tsv)`
     → `generators core builtin`.
   - Area 2's "no core dep" for this generator is wrong.
6. **Outputs.** `bazel-bin/spec/generated/DynaFn.java`: DynaFn.java with the member block rewritten.
7. **Committed?** Yes: `C/builtin/DynaFn.java`; writer `//core:update_generated_0`; diff test `_0_test` → `//:generated`;
   in `//:update_generated`.
8. **Who consumes it.**
   - Build: `//core:core_next`, `//spec:gen_claims` (override).
   - Product: core's DynaFn enum (`C/normalizer/RelOpTranslator.java` arms, per `T/generators/DynaFnRegistryTest.java:41`).
   - Tests: `DynaFnRegistryTest` (`:55-131`, which calls `DynaFnGenerator.pureFqns`), and `//spec:ratchets`'
     `dynafn.unsupported` (see that dossier).
9. **Determinism.** Yes: TreeMap and TreeSet; the walk order does not matter (the result is keyed by name). Per-name merge
   order is independent (`:73-86`).
10. **Cost.** One JVM at the default heap, reading about 3,253 `.pure` files fully. No database.
11. **What reruns it today.** Any file of either upstream archive, any core main source or resource, base, json, any
    `spec/src/gen/java` file, DynaFn.java.
12. **True trigger.**
    - an upstream bump (the relationalStore registry files);
    - hand edits to DynaFn.java (Resolution and Lite columns, the hand-written parts);
    - the chain's engine-handlers.tsv;
    - Pure's three SQL constants.
13. **Who runs it today.** The build lane, the checks lane, `//gates:local`, the bump, and by hand.
14. **Recommendation: COMMITTED-UPSTREAM.**
    - **Manual?** No.
    - **Writer:** chain update group (`//:update_generated` and the bump).
    - **Diff test:** everyday gate. DynaFn.java's hand columns are edited in ordinary commits.
    - **Narrowing:**
      - declare only `legend-engine-xts-relationalStore`'s `.pure` (and pass that subroot), not either whole tree;
      - its own library on `//core:builtin` only;
      - take `:gen_engine_handlers`' output as an input in place of the committed table (closes the chain, 0.4).
15. **Open questions.** None beyond 0.4.

## //spec:gen_engine_handlers

1. **Identity.** `_java_run`, `spec/BUILD.bazel:316-328`, not manual. Program `com.legend.generators.EngineHandlersGenerator`,
   entry `G/EngineHandlersGenerator.java:55`. Writer `//core:update_generated_3`.
2. **What it computes.** legend-engine resolves a bare function name against `Handlers.java`'s registry first. This table is
   that registry: each bare name with each signature id it stands for, and the FQN our platform declares for that id (empty
   when we carry none). Our product's own surface (`Pure.LITE_SURFACE`) is appended as `lite` rows.
3. **Why it exists.** `a77197dab` (2026-09-25) "The engine surface by bare name, generated from the pinned Handlers.java — a
   census, no consumer" (untangle step 4b.0). It replaced the unverified `FN_BY_BARE`. It is now consumed by `EngineHandlers`
   (`C/builtin/EngineHandlers.java`), `BareNames` (`C/compiler/BareNames.java:57`) and DynaFnGenerator. The reason is still
   live.
4. **Inputs, declared** (aq: 155):
   - upstream: 1 file (`Handlers.java`, `spec/BUILD.bazel:314`);
   - jars: 31 core jars plus 5;
   - JDK: 118 files;
   - no source files, and no other generator's output.
5. **Inputs, actually read.**
   - **Files:** Handlers.java.
   - **Compiled committed core:** `Pure.all()`, `Pure.LITE_SURFACE`, `Pure.Lite.PKG`, `Prelude.elements()` (the COMMITTED
     `RES/prelude.pure`, `C/builtin/Prelude.java:42`), `SignatureMangle` (`:81-89,110-123`).
   - **Over-declared:** core jars beyond builtin, model and parser (Prelude parses at class init), plus claims and
     source_tree.
   - **Under-declared, in the chain sense:**
     - its true inputs are the chain's Pure.java and prelude.pure, but it reads the committed copies through `//core`
       (`somepath(//spec:gen_engine_handlers, //core:src/main/resources/com/legend/builtin/prelude.pure)` → `generators core builtin`);
     - it even depends on its own committed output (`somepath(//spec:gen_engine_handlers, //core:src/main/resources/com/legend/builtin/engine-handlers.tsv)`
       → `generators core builtin`), never read by the program.
6. **Outputs.** `bazel-bin/spec/generated/engine-handlers.tsv`: a header comment, a column line, then `name id fqn source`
   rows sorted by name (836 rows at landing; 838 lines today).
7. **Committed?** Yes: `RES/engine-handlers.tsv`; writer `//core:update_generated_3`; diff test `_3_test` → `//:generated`;
   in `//:update_generated`.
8. **Who consumes it.**
   - Product, at run time: `EngineHandlers` and `BareNames`, the resolver's bare-name tier; it is embedded in the browser
     planner (`wasm/src/main/java/planner/PreludeResources.java:22`) and Studio's page
     (`sdlc-server/src/main/java/com/legend/sdlc/page/PageResources.java:15`).
   - Generators: gen_dynafn (committed copy).
   - Tests: `core/src/test/java/com/legend/builtin/EngineHandlersTest.java` (reads the resource, `:59`).
9. **Determinism.** Yes: TreeMap and TreeSet, `Pure.all()` order fixed.
10. **Cost.** One JVM; parses the committed 7,018-line prelude at `Prelude` class init. Small.
11. **What reruns it today.** Any core main source or resource (including its own committed output), base, json, any
    `spec/src/gen/java` file, Handlers.java.
12. **True trigger.** An upstream bump (Handlers.java), plus the chain's Pure.java and prelude.pure (which move on membership
    edits: `1446b82c0` regenerated it beside six new constants), plus `Pure.LITE_SURFACE`.
13. **Who runs it today.** The build lane, the checks lane, `//gates:local`, the bump, and by hand.
14. **Recommendation: COMMITTED-UPSTREAM.** Its own spliced inputs are the chain files Pure.java and prelude.pure.
    - **Manual?** No.
    - **Writer:** chain update group.
    - **Diff test:** everyday gate (it moves with membership edits).
    - **Narrowing:** depend on `//core:builtin` plus `//core:model`, and read the chain's outputs (gen_natives' Pure.java
      compiled, gen_prelude's prelude.pure) instead of the committed copies. This is the change that makes one update run
      enough (0.4); the design doc says the same (`BUILD_REBUILD_DESIGN_2026_10_05.md:191`).
15. **Open questions.** How to hand it a compiled regenerated Pure without a second core: settle by choosing between a small
    builtin-only `core_next` slice and an extraction step that emits the catalog's (id, fqn) list as data.

## //spec:gen_prelude

1. **Identity.** `_java_run`, `spec/BUILD.bazel:450-469`, not manual. Program `com.legend.generators.PreludeGenerator`, entry
   `G/PreludeGenerator.java:81`. Writer `//core:update_generated_5`.
2. **What it computes.** The "prelude" is the library of upstream classes, enums and functions our platform loads at boot.
   The generator decides which declarations belong:
   - legend-pure's platform packages whole;
   - every spec class or enum our Java NAMES in code;
   - what the system metamodel names;
   - engine natives;
   - the closure of all of these.
   It then copies each declaration verbatim, under its file's imports, into one ordered module. It leaves out shapes Pure.java
   still declares by hand, names the platform claims, and excluded packages. It refuses unprovenanced hand classes and
   missing upstream files (`:233-237,254-260,423-450`).
3. **Why it exists.** `7e2b32ae3` (2026-09-04, batch 54) "the prelude's library shapes are generated from the spec"; the
   module form in `4cfc206ee` (2026-09-08); a program and Bazel action since `998f1a41c`. It replaces hand-typed library
   shapes with spec data. The reason is still live.
4. **Inputs, declared** (aq: 16,499):
   - upstream: both trees (12,900 + 2,693);
   - our sources: `//core:main_java`, every file under `core/src/main/java` (751 files, all `.java`; `find core/src/main/java -type f` = 751 = `-name '*.java'`);
   - other generators' outputs: `:gen_natives`' Pure.java (1);
   - jars: 31 core jars plus 5;
   - JDK: 118 files.
5. **Inputs, actually read.**
   - **Files:**
     - the 3 engine spec roots' `.pure` and the whole pure tree's `.pure` (index, `:173-195`);
     - `m3.pure` (`:201,214`);
     - the relational corpus directory (`:245-247`);
     - `UpstreamFiles.LIBRARY_FILES` and `SHAPE_FILES` (`:248-253`), `PLATFORM_ROOTS` (`:390,1364-1376`);
     - the files declaring engine natives (`:1390-1400`);
     - every `core/src/main/java/**/*.java` except Prelude.java (`:338-357`), with Pure.java taken from the override.
   - **Compiled committed core:** Pure, SystemMetamodel, CoreFn, NameResolver.CORE_IMPORTS, our parser, and Claims compiled
     against `//core`.
   - **Over-declared:**
     - engine files outside the spec roots and corpus;
     - many core jars (e.g. driver and server).
   - **Under-declared, in the chain sense:**
     - committed DynaFn.java and NameResolver.java text are read where the chain's versions should be;
     - compiled Pure, CORE_IMPORTS and Claims come from committed core;
     - the committed prelude.pure is loaded through NameResolver's class init (`somepath(//spec:gen_prelude, //core:src/main/resources/com/legend/builtin/prelude.pure)`
       → `generators core builtin`).
   - **Optional `census` path:** `main` passes null (`:91-92`); no caller in the repo passes a path (`git grep "PreludeGenerator.generate("` finds none outside the class), so the `-Dprelude.census` write the javadoc mentions (`:62-67`) is dead.
6. **Outputs.** `bazel-bin/spec/generated/com/legend/builtin/prelude.pure`: the module, a header plus one `###Pure` section per
   (spec file, import scope), with receipt lists at the foot (7,018 lines today).
7. **Committed?** Yes: `RES/prelude.pure`; writer `//core:update_generated_5`; diff test `_5_test` → `//:generated`; in
   `//:update_generated`.
8. **Who consumes it.**
   - Build: `//core:core_next_prelude` (→ gen_claims' classpath).
   - Product, at run time: `Prelude` (the boot layer, `C/builtin/Prelude.java:42`), `NameResolver` and `Compiler`
     (`C/Compiler.java:279-338`); embedded in wasm and Studio's page (the files above).
   - Generators: gen_engine_handlers (committed copy).
   - Tests: `T/generators/PreludeGeneratorTest.java` (two unit checks, `:33-55`), and indirectly every compile test.
9. **Determinism.** Designed so:
   - module order is a stated rule (`:655-660`);
   - TreeMap and TreeSet indexes, sorted walks;
   - paths written relative to the checkout (`relative`, `:1587-1597`).
   HashMap `byBare` is used for lookups only, and its lists follow the TreeMap order. No time or randomness.
10. **Cost.** The heaviest link: one JVM at the default heap, regex-indexing about 2,265 `.pure` files, scanning 750 Java
    files, and parsing and resolving the closure's files. No database.
11. **What reruns it today.**
    - any upstream file;
    - any byte of any core main Java file, comments included (text input, design doc R6);
    - any core resource, base, json, any `spec/src/gen/java` file;
    - gen_natives' output.
12. **True trigger.**
    - an upstream bump;
    - changes to which spec FQNs our core Java names in code;
    - Pure.java's hand-declared classes and enums;
    - SystemMetamodel's source;
    - CoreFn names;
    - the claimed bare names (registries);
    - CORE_IMPORTS.
    Its 2 commits since 2026-09-23 (`e2f201210`, `cef2288a4`) were both product changes, not bumps.
13. **Who runs it today.** The build lane, the checks lane, `//gates:local`, the bump, and by hand.
14. **Recommendation: COMMITTED-SOURCE.** Its true trigger includes named files of ours (the demand scan of core Java,
    Pure.java hand shapes, SystemMetamodel, CoreFn, the registries) plus the bump.
    - **Manual?** No.
    - **Writer:** chain update group (`//:update_generated` and the bump).
    - **Diff test:** everyday gate, where core changes are checked.
    - **Narrowing:**
      - one small extraction step turns core's Java into the set of FQN tokens and hand-declared names it reads, so a
        comment edit stops at the extraction (design doc `:201`);
      - its library on the parser, builtin and platform slices;
      - declare only the spec roots, the corpus and the platform roots;
      - read the chain's DynaFn.java and NameResolver.java and the regenerated Claims (0.4).
15. **Open questions.**
    - Whether the committed prelude loaded via NameResolver's class init can change its output: settle by tracing which
      `NameResolver.resolve` overload `Spec` calls and its `preludeOn` value.
    - Whether `Claims` needs the regenerated core: settle by checking whether any claimed bare name can change from a chain
      output.

## //spec:gen_claims

1. **Identity.** `_java_run`, `spec/BUILD.bazel:491-512`, not manual. Program `com.legend.claims.ClaimsGenerator`, entry
   `CL/ClaimsGenerator.java:38`, compiled in `//spec:claims_generator_lib` against `//core:core_next`
   (`spec/BUILD.bazel:474-487`). Writer `//core:update_generated_4`.
2. **What it computes.** A ledger with one row per Pure.java overload:
   - its FQN, signature, constant name(s);
   - which lowering registries or families claim it, and their owners;
   - the `also` column: which main source files name it, by `Pure.X` or by quoted FQN.
   UNCLAIMED marks an overload nothing implements.
3. **Why it exists.** `3e0435666` (2026-09-10) "Batch 3 (upstream boundary): the claim registry — the implemented surface as a
   computed fact"; a program on core_next since `998f1a41c`. Its own header says it is RETIRING at untangle step 5
   (`CL/ClaimsGenerator.java:209-210`; `core/BUILD.bazel:87-88`). Its original reason, a reviewable surface while batch 4
   emptied UNCLAIMED, is met (`UNCLAIMED_MAX = 0` since 2026-09-10, `T/claims/ClaimRegistryTest.java:83-84`).
4. **Inputs, declared** (aq: 878):
   - our sources: `//core:main_java` (751);
   - other generators' outputs: `:gen_dynafn`, `:gen_imports`, `:gen_natives` (3), and through jars `libcore_next.jar` and
     `libcore_next_prelude.jar` (2);
   - jars: base, json, claims_generator_lib, source_tree (4);
   - JDK: 118 files;
   - no upstream files directly (only through the gen_* outputs).
5. **Inputs, actually read.**
   - **Compiled core_next:** Pure's fields by reflection (`:58-87`); Claims' static init over `RegistryKeys`, `CoreFn`,
     `Pure.walledNativeFqns`, `NativeFn.families` (`CL/Claims.java:91-128`).
   - **Text:** every `.java` under the core source root, with the three overrides (`:93-103`).
   - **Over-declared:**
     - core_next's resources: the committed engine-handlers.tsv, native-membership.tsv, and its own committed output
       native-claims.tsv (`somepath(//spec:gen_claims, //core:src/main/resources/com/legend/builtin/native-claims.tsv)`
       → via core_next);
     - core_next_prelude: OPEN whether any class Claims loads touches `Prelude` or `EngineHandlers`; settle with
       `-verbose:class`.
   - **Under-declared:** none.
6. **Outputs.** `bazel-bin/spec/generated/native-claims.tsv`: a header line, a column line, then about 831 rows sorted by FQN
   and signature (833 lines today).
7. **Committed?** Yes: `RES/native-claims.tsv`; writer `//core:update_generated_4`; diff test `_4_test` → `//:generated`; in
   `//:update_generated`.
8. **Who consumes it.** No code reads it as data. The only opener is `_4_test`. See 0.5 for every reader, including the unused
   `ClaimRegistryTest.RESOURCE` and docs.
9. **Determinism.** Mostly:
   - TreeMap-sorted rows, sorted source walk (`:89-92`), `\n` line ends (`:49-52`);
   - one theoretical hazard: `Pure.class.getFields()` order is unspecified by the JDK. It sets the order of constant names
     joined with `|` when two constants alias one overload (`:58-66,224`). HotSpot returns declaration order in practice.
10. **Cost.**
    - Its own action is small: reflection, then about 831 overloads × 751 files of substring search.
    - It forces `//core:core_next`: a second NullAway compile of 751 files, plus core_next_prelude.
11. **What reruns it today.**
    - any core main Java file (compile and text);
    - any core main resource, including its own committed output (path above);
    - base, json, Claims.java, ClaimsGenerator.java, SourceTree.java;
    - any output change of gen_natives, gen_imports, gen_dynafn or gen_prelude (Bazel cuts off when their bytes are
      unchanged).
12. **True trigger.**
    - changes to the claim sources (registries, CoreFn, walls, NativeFn families);
    - adding or removing a reference to a Pure constant or FQN in core main Java;
    - the chain's Pure.java.
13. **Who runs it today.** The build lane, the checks lane, `//gates:local` (via `_4_test`), the bump, and by hand.
14. **Recommendation: DRAFT-MANUAL if D2 keeps it; DEAD if D2 retires it.** It is not product data and no test reads it
    (0.5). Its value is a report for a reviewer.
    - **Manual?** Yes.
    - **Writer:** neither group. Do not commit it.
    - **Diff test:** none.
    - **Narrowing:** build it on demand (`bazel build //spec:gen_claims`) against committed `//core` through the existing
      `//spec:claims` library, with no overrides. Delete `//core:core_next`, `//core:core_next_prelude`,
      `//spec:claims_generator_lib` and `//core:update_generated_4(_test)`.
    - **What is lost:** the forcing function that turns `//:generated` red when the surface or its references change (0.5).
      The UNCLAIMED ratchet stays live in ClaimRegistryTest.
15. **Open questions.**
    - D2 itself (the user's decision).
    - If kept committed, whether the diff test is worth a second core: settle by the user choosing between forced review
      and build cost.

## //spec:native_declarations

1. **Identity.** `_java_run`, `spec/BUILD.bazel:417-433`, NOT manual. Program `com.legend.generators.NativeDeclarations`,
   entry `G/NativeDeclarations.java:29`.
2. **What it computes.** For every FQN in native-membership.tsv, every upstream declaration of it: FQN, canonical key,
   canonical text and source file, one per line. It is what a person re-keying membership rows reads.
3. **Why it exists.** `65b9e2a69` (2026-10-04, P2-17): it replaced NativeSignatureGeneratorTest's `-Dnatives.dump=<file>`,
   which wrote anywhere (commit message; `G/NativeDeclarations.java:15-20`). The reason is still live while membership
   re-keying legs happen.
4. **Inputs, declared** (aq: 15,748):
   - upstream: both trees (12,900 + 2,693);
   - our sources: `RES/native-membership.tsv` (1);
   - jars: 31 core jars plus 5;
   - JDK: 118 files.
5. **Inputs, actually read.**
   - Exactly gen_natives' upstream reads: it calls `NativesGenerator.upstreamDeclarations` (`:35`), which uses the same
     compiled committed parser and NameResolver.
   - The membership TSV. Not Pure.java.
   - **Over-declared:** as gen_natives (the engine files outside the spec roots, non-`.pure` files, most core jars, claims).
   - **Under-declared:** none.
6. **Outputs.** `bazel-bin/spec/generated/native-declarations.tsv`. Its file column is the exec-root-relative path
   (`external/+http_archive+legend_engine_src/…`, `:38-39`).
7. **Committed?** No.
8. **Who consumes it.**
   - Build: nothing (`bazel query 'rdeps(//..., //spec:native_declarations, 1)'` → only `//spec:guard_classpaths`, a FileWrite
     with no inputs, per `bazel aquery 'deps(//spec:guard_classpaths, 0)'`).
   - Humans: `docs/GATES.md:65-66` ("`bazel build //spec:native_declarations` lists every upstream declaration of a
     membership FQN"), `spec/BUILD.bazel:415-416`.
9. **Determinism.** Yes: insertion order follows sorted walks. The paths depend on Bazel's canonical repo name
   `+http_archive+…`, which is stable but changes if that naming changes.
10. **Cost.** As gen_natives (about 2,265 `.pure` read, the subset parsed), at the default heap.
11. **What reruns it today.** Any upstream file, native-membership.tsv, any core main source or resource, base, json, any
    `spec/src/gen/java` file. It runs in every `bazel build //...` although nobody asked.
12. **True trigger.** A human request (a re-keying leg or a provenance review).
13. **Who runs it today.** Only `bazel build //...` (the CI build lane), where it is wasted work. It is not in `//gates:local`
    or `//:generated`, and not in the bump. By hand per GATES.md:65.
14. **Recommendation: DRAFT-MANUAL.**
    - **Manual?** Yes (`tags = ["manual"]`; the design doc agrees, `BUILD_REBUILD_DESIGN_2026_10_05.md:184`).
    - **Writer:** none (a build output).
    - **Diff test:** none.
    - **Narrowing:** the same library and tree narrowing as gen_natives.
15. **Open questions.** None.

## //spec:native_membership_draft

1. **Identity.** `_java_run`, `spec/BUILD.bazel:437-445`, NOT manual. Program `com.legend.generators.NativeMembershipDraft`,
   entry `G/NativeMembershipDraft.java:37`. Its writer is `//core:draft_native_membership` (`write_source_files`,
   `diff_test = False`, `core/BUILD.bazel:832-836`), also NOT manual (`bazel query 'attr(tags, manual, //core:all)'` does not
   list it).
2. **What it computes.** A draft of the hand-owned native-membership.tsv from Pure.java's compiled constants: every public
   static `NativeFunctionDefinition` outside `Pure.Lite`, keyed by its canonical signature, sorted by FQN then key
   (`:46-80`). A person finishes it; `git diff` shows what moved.
3. **Why it exists.** `65b9e2a69` (2026-10-04, P2-17): it replaced the test's `-Dnatives.bootstrap`, which wrote into the
   source tree through runfiles. Per the commit message, "Today's draft equals the committed file but for one hand comment
   line and three rows' order." The reason is live only for a bootstrap or a mass re-key; membership is otherwise edited by
   hand.
4. **Inputs, declared** (aq: 154): 31 core jars plus base, json, claims, generators, source_tree (5), and JDK 118 files. No
   files.
5. **Inputs, actually read.**
   - **Compiled committed `Pure`:** reflection over `getDeclaredFields`, plus `NativesGenerator.canonicalKey`, which parses
     nothing (it works on the loaded definitions).
   - **Over-declared:** every core jar beyond builtin, model and protocol, and claims and source_tree
     (`somepath(//spec:native_membership_draft, //core:src/main/java/com/legend/server/DiagramService.java)` →
     `generators core server_lib`).
   - **Under-declared:** none.
6. **Outputs.** `bazel-bin/spec/generated/native-membership.draft.tsv`: a 3-line header comment, then
   `constant fqn signatureKey` rows.
7. **Committed?** No as a generated file. `//core:draft_native_membership` writes it OVER the committed hand-owned
   `RES/native-membership.tsv` when a human runs it. It has no diff test and is outside `//:update_generated`
   (`core/BUILD.bazel:829-831`).
8. **Who consumes it.**
   - Build: only `//core:draft_native_membership` (rdeps query).
   - Humans: `docs/GATES.md:63-64`, `G/NativeMembershipDraft.java:24-27`.
   - The check on the hand file is `T/generators/NativeSignatureGeneratorTest.java:47-66`, which uses
     `NativeMembershipDraft.constants()`, not this output.
9. **Determinism.** Yes: TreeMap of fields, then sorted.
10. **Cost.** Small: one JVM and class init of Pure.
11. **What reruns it today.** Any core main source or resource, base, json, any `spec/src/gen/java` file. It is built by every
    `bazel build //...` (its own target and the writer's runfiles).
12. **True trigger.** A human request.
13. **Who runs it today.** `bazel build //...` (the CI build lane; wasted). It is not in `//gates:local`, `//:generated` or
    the bump. By hand: `bazel run //core:draft_native_membership` (GATES.md:63-64).
14. **Recommendation: DRAFT-MANUAL.**
    - **Manual?** Yes, and so should `//core:draft_native_membership` be.
    - **Writer:** neither group (as today).
    - **Diff test:** none (as today).
    - **Narrowing:** its own small library on `//core:builtin`.
15. **Open questions.** Whether the draft is still worth keeping, given that membership is hand-edited row by row and the
    draft already differs from the committed file: settle by asking the owner whether a bootstrap or mass re-key is still
    foreseen.

---

## //spec:ratchets

1. **Identity.** `java_run` (//tools/java_run), `spec/BUILD.bazel:394-405`, `testonly = True`, NOT manual
   (`bazel query 'attr(tags, manual, //spec:all)'` does not list it). Program `com.legend.generators.SpecRatchets`,
   entry `T/generators/SpecRatchets.java:33`. Writer `//spec:update_ratchets` (`spec/BUILD.bazel:407-413`).

2. **What it computes.** Ten integers, one `key<TAB>value` per line, sorted (TreeMap): the relational corpus's
   census (how many `<<test.Test>>` functions upstream declares, how many are ToFix/ExcludeAlloy, the difference), how
   many DynaFn members are UNSUPPORTED, how many implementation-table rows of each kind there are, and how many
   hard-coded upstream paths the spec tests use. Tests compare their live value with the committed copy.

3. **Why it exists.** Commit `c3ea991f7` (2026-10-05) "Spec's hand-copied measurements become a generated report
   (P2-16 (b), spec)"; workplan P2-16 / decision D9 (b): a count a test pinned by a hand-copied constant is measured into
   a generated, diff-tested file; ceilings stay dated Java constants (`SpecRatchets.java:17-24`). It replaced the
   constants DISCOVERED/DECLARED/EXCLUDED, PINNED_COUNT and the kinds Map (commit message). Reason still live.

4. **Inputs, declared** (aq, 15,777 inputs): upstream `@legend_engine_src//:tree` 12,900 files + `@legend_pure_src//:tree`
   2,693 (UPSTREAM_TREES, `spec/BUILD.bazel:397`); 33 core jars (31 layer jars + `libduckdb_load`, `libshadow_binding`
   via `spec_tests_lib` runtime_deps `spec/BUILD.bazel:95-98`); our jars base, json, claims, generators, source_tree,
   spec_tests_lib, testing, tools/junit (8); 25 third-party jars (JUnit 5 platform x12, ArchUnit x5, opentest4j,
   apiguardian, slf4j, DuckDB/H2/Postgres/SQLite JDBC, rules_java runfiles); JDK 118 files. No source files directly;
   no other generator's output. JVM flags `-Dlegend.engine.root=…/pom.xml -Dlegend.pure.root=…/pom.xml
   -Duser.timezone=GMT` (`program_jvm_flags("spec")`, `tools/generators/defs.bzl:26-44`).

5. **Inputs, actually read.**
   - `corpus.census.*`: `MinimalCorpusTest.scanCensus()` walks `Corpus.RELATIONAL` = engine root +
     `UpstreamFiles.RELATIONAL` (`T/rcorpus/MinimalCorpusTest.java:872-895`, `T/rcorpus/Corpus.java:48-50`), root
     from `-Dlegend.engine.root` (`ProgramPaths.rootOf`).
   - `dynafn.unsupported`: compiled `com.legend.builtin.DynaFn` of committed core (`SpecRatchets.java:44-45`), i.e.
     the committed, generated `DynaFn.java`.
   - `implementation.kinds.*`: `ImplementationTableTest.build()` (`T/generators/ImplementationTableTest.java:40-72`):
     `UpstreamDeclarations.load()` (both trees, `T/generators/UpstreamDeclarations.java:45-46`), compiled `Pure.all()`,
     `CoreFn`, `Pure.walledNativeFqns()`, `WalledBodies`, `Subsumed`, `PlatformRegistrations.current()`.
   - `upstream.paths`: `UpstreamPathManifestTest.manifest().size()` (`T/generators/UpstreamPathManifestTest.java:51-88`):
     a count of OUR constant lists (Corpus.LIBRARY_FILES, SHAPE_FILES, PLATFORM_ROOTS, ENGINE_SPEC_ROOTS, ...); it
     builds paths but does not open them.
   - Never reads the committed ratchets.tsv (`SpecRatchets.java:57-58`).
   - **Over-declared:** the committed `ratchets.tsv` and every other spec test resource ride in `spec_tests_lib`'s jar
     (`spec/BUILD.bazel:94`), e.g. `bazel query 'somepath(//spec:ratchets, //spec:src/test/resources/com/legend/generators/ratchets.tsv)'`
     and the reference-lane golden (`somepath(//spec:ratchets, //spec:src/test/resources/reference-lane/core_relational.txt)`
     → via `//spec:spec_tests_lib`); the 4 JDBC drivers, ArchUnit, JUnit (unused by the four measurements; OPEN: confirm
     no static init in MinimalCorpusTest pulls them — class-loading MinimalCorpusTest loads JUnit annotations at most);
     most of the 33 core jars (e.g. `server_lib`: `somepath(//spec:ratchets, //core:src/main/java/com/legend/server/DiagramService.java)`
     → spec_tests_lib → //core:core → server_lib). **Under-declared:** none found (all reads are classpath or the two
     declared trees).

6. **Outputs.** `bazel-bin/spec/generated/ratchets.tsv`: a header comment plus 10 rows today (committed copy:
   corpus.census.declared 2761, .discovered 2613, .excluded 148, dynafn.unsupported 41, implementation.kinds.Body 2187,
   .Form 217, .Intrinsic 671, .Refused 20, .Unimplemented 71, upstream.paths 91).

7. **Committed?** Yes: `spec/src/test/resources/com/legend/generators/ratchets.tsv`; writer `//spec:update_ratchets`
   (write_source_files, `spec/BUILD.bazel:407-413`); diff test `//spec:update_ratchets_test` in suite
   `//spec:update_ratchets_tests`; in `//:generated` (`BUILD.bazel:93`) and `//:update_generated` (`BUILD.bazel:121`).

8. **Who consumes it.** The committed copy, as a classpath resource (`SpecRatchets.java:64`), through
   `SpecRatchets.measured/measuredWithPrefix`:
   - `UpstreamPathManifestTest` `T/generators/UpstreamPathManifestTest.java:108` (`upstream.paths`) — in `//spec:spec_tests`.
   - `DynaFnRegistryTest` `T/generators/DynaFnRegistryTest.java:104` (`dynafn.unsupported`) — spec_tests.
   - `ImplementationTableTest` `T/generators/ImplementationTableTest.java:127` (`implementation.kinds.`) — spec_tests.
   - `MinimalCorpusTest` `T/rcorpus/MinimalCorpusTest.java:112` (`discovered()`), `:862-863` (pinCensus, called at
     `:155`), `:908-910` (pinRoster denominator) — `@Tag("heavy")`, runs inside the corpus-lane judge actions
     (`spec/corpus.bzl:9` selects the class), lanes 4 and 5.
   No product reader, no other generator. Humans: the diff test's message (`spec/BUILD.bazel:410`). Note: every row
   group has a reader test asserting live == committed, so the diff test duplicates them; the census is checked only in
   the corpus lanes.

9. **Determinism.** Integers in a TreeMap, `'\n'` (`SpecRatchets.java:37-54`); census counts files regardless of walk
   order. Deterministic.

10. **Cost.** One JVM, `memory_mb` unset → JVM default heap (aq: no -Xmx). Walks the relational corpus with regexes,
    loads every upstream declaration (`UpstreamDeclarations.load`), builds the implementation table. No database or
    server. (OPEN: heap peak; settle with `-Xlog:gc` on one run.)

11. **What reruns it today.** Any change to either upstream tree, any core main source (33 jars), any spec test source
    or resource (spec_tests_lib), //testing, //tools/junit, JDBC/JUnit pools. Surprising: re-blessing the reference
    lane golden or the ratchets.tsv itself reruns it (queries in 5).

12. **What SHOULD rerun it.** Per row: an upstream bump (census; upstream half of the implementation table);
    `DynaFn.java` (dynafn.unsupported — generated, plus hand Resolution edits); our catalog and registrations
    (`Pure.java`, `CoreFn`, `WalledBodies`, `Subsumed`, lowering `PlatformRegistrations`) for implementation.kinds; our
    spec test constants (`Corpus`, `UpstreamFiles`, `PreludeGenerator.ENGINE_SPEC_ROOTS`, `CoreImportsParityTest`) for
    upstream.paths. (The design table agrees: "triggers are an upstream bump, DynaFn.java, or the catalog",
    BUILD_REBUILD_DESIGN_2026_10_05.md:194.)

13. **Who runs it today.** `bazel build //...` (not manual) → CI lane `build` (`.github/workflows/gates-run.yml:63`).
    Its diff test via `//:generated` → CI lane `checks` (`gates-run.yml:51`) and `//gates:local` (`gates/BUILD.bazel:15`).
    The bump: `//:update_generated` (Bump.java:148-151) rewrites it, then `bazel test //...` (Bump.java:153-156).
    Readers: lane 3 (spec_tests), lanes 4/5 (corpus).

14. **Recommendation: COMMITTED-SOURCE** (mixed trigger: our catalog/registrations/DynaFn/test constants, plus the bump
    for the census and upstream declarations). It is a committed golden of counts; D9 says a move is a reviewed diff
    with its reason, so it should not be rewritten silently by an all-files update. Generator: `manual` is fine (the
    diff test pulls it). Writer: out of `//:update_generated`; run deliberately (`bazel run //spec:update_ratchets`),
    and by the bump as an explicit step that prints moved keys. Diff test: everyday gate (`//:generated`), though it
    only duplicates the four readers' asserts — dropping it is an option. Narrowing: an own small library
    (SpecRatchets + the census scan + ImplementationTable builder + manifest), depending on core's builtin/platform/
    lowering layers and //testing, not on `spec_tests_lib` (which carries its own committed output and the reference
    golden) nor the JDBC/JUnit/ArchUnit pools.

15. **Open questions.**
    - Is the diff test needed when every row has a reader asserting live == committed? Settle: decide whether the
      census (read only in lanes 4/5) must be checked in the everyday gate.
    - Should the bump re-bless it silently? Settle: user decision (D9 "reason in the commit" vs Bump.java phase 2).
    - Heap/time: one measured run.

---

## //spec:reference_lane_report (and //tools/reference:ref_dump)

1. **Identity.** `java_run`, `spec/BUILD.bazel:235-258`, `testonly`, `tags = ["manual"]`, mnemonic Generate. Program
   `com.legend.generators.ReferenceLaneReport`, entry `T/generators/ReferenceLaneReport.java:40`. Upstream of it:
   `//tools/reference:ref_dump`, `java_run`, `tools/reference/BUILD.bazel:64-84`, testonly, manual, mnemonic
   ReferenceDump, program `RefResolutions` (`tools/reference/RefResolutions.java:34`), library `:ref_resolutions`
   (`tools/reference/BUILD.bazel:51-58`).

2. **What it computes.** ref_dump runs legend-pure's own compiler from the pinned Maven jars over core_relational's
   27-module closure and dumps what every call resolved to (`ref-resolutions.tsv`, 235,668 rows at W1.1, GATES.md:6198).
   The report types the same closure with OUR compiler (`OurResolutions.dump`), joins call by call (`ReferenceJoin`),
   and writes a sorted report: coverage counts, bucket counts (AGREE/OVERLOAD/ABSENT/EXTRA/...), dropped sources, failed
   bodies, and every disagreement class with its count; plus an examples TSV with positions.

3. **Why it exists.** Execution plan W1.1 (reference lane): commits `b3af51609` (H3 spike, ref jars in Bazel),
   `06eeb8142` (2026-09-29, W1.1 (1) the lane), `3eebac61d` (2026-10-04, P2-14: the report becomes a Bazel action and
   the golden moves only through `bazel run //spec:update_reference_lane`; before, the test pinned it and a recipe
   unzipped test.outputs). Protects: our front end's resolution against legend-pure's, with coverage pinned so lost
   coverage turns red (GATES.md:6217-6219). Reason live.

4. **Inputs, declared** (aq, 15,779): both upstream trees (12,900 + 2,693; `spec/BUILD.bazel:238`); 33 core jars, the
   same 8 own jars and 25 third-party jars as :ratchets (deps `:spec_tests_lib`, `:257`); `//tools:oracle-pins.env`;
   `//tools/reference:ref_dump`'s `ref-resolutions.tsv`. ref_dump's own inputs: `RefResolutions.java` compiled against
   37 `@maven_upstream` jars (`tools/reference/BUILD.bazel:11-49`: eclipse-collections x2, legend-engine pure/xt
   modules x19, legend-pure m2/m3/m4 x16), 8192 MB resource set (`:79`), `-Xss16m`.

5. **Inputs, actually read.** `args[0]` ref-resolutions.tsv (`ReferenceJoin.join`), `args[1]` oracle-pins.env, only
   for `LEGEND_PURE_RELEASE`/`LEGEND_ENGINE_RELEASE` in the header (`ReferenceLaneReport.java:45-60`); both trees via
   `-Dlegend.engine.root`/`-Dlegend.pure.root` (`T/generators/OurResolutions.java:50-53`: the manifest closure of
   core_relational, walking each module root); compiled core (parser, compiler). **Over-declared:** spec_tests_lib's
   other classes/resources (ratchets.tsv: `bazel query 'somepath(//spec:reference_lane_report, //spec:src/test/resources/com/legend/generators/ratchets.tsv)'`;
   its own committed golden is also in that jar — OPEN: harmless but a re-bless reruns the report), JDBC drivers,
   ArchUnit. Under-declared: none found.

6. **Outputs.** `reference-lane/core_relational.txt` (the pinned report, no positions) and
   `reference-lane/core_relational-examples.tsv` (one example per class, with positions; nothing reads it but humans).

7. **Committed?** The report: yes, `spec/src/test/resources/reference-lane/core_relational.txt` (3,670 lines); writer
   `//spec:update_reference_lane` (manual, `spec/BUILD.bazel:262-268`), diff test `//spec:update_reference_lane_test`
   (manual; suite `update_reference_lane_tests`). In neither `//:generated` nor `//:update_generated`. The examples:
   not committed. ref-resolutions.tsv: build output, never committed.

8. **Who consumes it.** `//spec:reference_lane` (junit_test, manual, `spec/BUILD.bazel:271-279`) → `ReferenceLaneTest`
   (`T/generators/ReferenceLaneTest.java:37-59`): every disagreement class must match a row of
   `spec/src/test/resources/reference-lane/reasons.tsv` (26 lines). The diff test against the golden. Humans: GATES.md
   entries cite runs (5835, 5860, 5897, 5946, 6013-6019, 6082, 6196-6229, 6751-6765); `spec/BUILD.bazel:232-233`
   "every front-end slice's GATES entry cites a run". ref-resolutions.tsv consumers: only this report (`bazel query
   'rdeps(//..., //tools/reference:ref_dump)'`); also by hand `tools/reference/join.py` and `source_drift.py`
   (usage lines 10, 8) and the README recipe (`tools/reference/README.md:5,39`).

9. **Determinism.** Report: "deterministic, sorted, no positions" (`ReferenceLaneReport.java:72`); two runs
   byte-identical (GATES.md:6227). ref_dump: NOT byte-reproducible ("two runs on the same inputs differ … cached, never
   compared", `tools/reference/BUILD.bazel:60-63`). Examples TSV: OPEN (an example per class may depend on ref_dump's
   row order).

10. **Cost.** Report: JVM `-Xmx2048m -Xss16m` (`memory_mb = 2048`, aq). ref_dump: 8192 MB scheduled, peak 3,072 MB live
    (`tools/reference/BUILD.bazel:78`), ~35-50 s. Together "about 8 GB" (`spec/BUILD.bazel:234`). No database.

11. **What reruns it today.** Report: either tree, any core main source, any spec test source/resource, //testing,
    oracle-pins.env (a pin change, i.e. the bump), ref_dump's output. ref_dump: only RefResolutions.java and the 37
    jars (design: "already right", BUILD_REBUILD_DESIGN:181).

12. **What SHOULD rerun it.** Our front end (parser, resolver, typer in //core's compiler layers) and an upstream bump
    (trees + jars + pins). It judges our compiler's behaviour against the reference: engine behaviour.

13. **Who runs it today.** Nobody automatically: manual, so not in `bazel build //...`; in no CI lane
    (`gates-run.yml:50-65`), not in `//gates:local`, not in Bump.java (manual targets are outside `//...`). By hand:
    `bazel test //spec:reference_lane //spec:update_reference_lane_test` (`ReferenceLaneTest.java:27`). Evidence of the
    cost of that: the lane was red from `c1f9bac5b` (2026-10-01) to 2026-10-05 unnoticed (GATES.md:6753).

14. **Recommendation: TEST-IN-DISGUISE** (already shaped as a manual test lane: the report is the measurement step of
    a golden diff test plus the reasons invariant). Keep `manual` (8 GB). Writer: neither update group; re-bless only
    deliberately (as today). Diff test and `//spec:reference_lane`: their own lane — propose a scheduled or
    front-end-path-triggered CI lane so a red is seen in hours, not days. Narrowing: a `reference` test library split
    from spec_tests_lib (OurResolutions, ReferenceJoin, the manifest-closure helper, ReferenceLaneReport) on core's
    compiler layers only, so spec-test edits and ratchets/golden re-blesses stop rerunning it.

15. **Open questions.** Is the examples TSV deterministic (settle: two runs with a cold ref_dump)? Should the lane run
    in CI (settle: user decision; runner memory 7 GB macOS vs 8 GB need)? Does a ref_dump rebuild ever change the
    report (settle: report from two cold dumps, per W1.1d, EXECUTION_PLAN_2026_09_26.md:532-533)?

---

## //spec:eager_corpus_compile

1. **Identity.** `java_run` from a list comprehension, `spec/BUILD.bazel:517-533` (name at `:531`), testonly,
   `tags = ["manual"]`, mnemonic Measure. Program `com.legend.rcorpus.EagerCorpusCompileProbe`, entry
   `T/rcorpus/EagerCorpusCompileProbe.java:34`.

2. **What it computes.** Builds the relational corpus's compiled world (`new MinimalCorpus()`) and types every function
   body up front (`Compiler.compileAllBodies`), then reports how many bodies fail, grouped by reason class, package,
   source file and "family" (test bodies; engine machinery walled by file in `WALLED_FILES`; the RESIDUE, ours to fix).
   A measurement: it never fails on a count.

3. **Why it exists.** Batch 169 (2026-09-09, GATES.md:445): the user's ask "we need to know everything compiles when
   needed", run by name, "NOT a gate — do all the work, then decide" (`EagerCorpusCompileProbe.java:12-20`;
   COMPILE_EVERYTHING_HOMEWORK_2026_09_09.md:149, 213-216; LEDGER_GRANULAR_2026_09_06.md:1003). File first committed in
   `756a63666` (2026-09-11, spec module). Became a report action in `0ad319cfd` (2026-10-05, P3-17 second part:
   "Measurements are report actions, probes are binaries; no test that only prints"). Reason: still a pending user gate
   decision; the probe has had no reader since.

4. **Inputs, declared** (aq, 17,190): both upstream trees (12,900 + 2,693); 1,413 source files = `:srcs` (all of
   spec/src) + `//core:srcs` (all of core/src incl. tests, `spec/BUILD.bazel:116-119,520`); 33 core jars, 8 own jars,
   25 third-party jars (deps `:spec_tests_lib`); JDK.

5. **Inputs, actually read.** Engine tree via `-Dlegend.engine.root` (`MinimalCorpus()` reads `Corpus.RELATIONAL`
   files and shared sources, `T/rcorpus/MinimalCorpus.java:204-235`); compiled core; `-Deager.world2` (absent here).
   Output dir `args[0]`. **Over-declared:** the 1,413 source files (`:srcs` + `//core:srcs`): nothing in the probe or
   MinimalCorpus reads repo files (no `core/src`/`spec/src`/`legend.sources` reference in `T/rcorpus/*.java`); e.g.
   `bazel query 'somepath(//spec:eager_corpus_compile, //core:src/test/java/com/legend/HarnessDisciplineTest.java)'`
   and `somepath(//spec:eager_corpus_compile, //core:src/main/resources/com/legend/builtin/native-claims.tsv)` →
   `//core:srcs`. The legend-pure tree (only world2 reads it, `:113`). JDBC drivers: the corpus's session supplier is
   lazy (`MinimalCorpus.java:344`), OPEN whether the runner opens one at construction. Under-declared: none.

6. **Outputs.** `eager_corpus_compile/eager-corpus.txt` (summary header lines — families, residue by source, by source
   failed/total, totals with timings, by reason, by package — then every failing body with its error);
   `eager-residue.txt` (one line per RESIDUE body with its error); `run.log` (the console, `Programs.captureConsole`,
   `testing/.../Programs.java:25-31`).

7. **Committed?** No. Consumed by nothing in the build: `bazel query 'rdeps(//..., //spec:eager_corpus_compile)'`
   returns only the guard reports (`//spec:guard_classpaths`, `guard_markdown` → `//tools/guards:*` → `//gates:local`),
   which read the target's classpath provider, not its outputs (`tools/guards/classpath.bzl:41-50`).

8. **Who consumes it.** Nobody reads the output files: grep of the repo for `eager-corpus`/`eager-residue` finds only
   the BUILD line and historical docs citing numbers (GATES.md:439, 445; COMPILE_EVERYTHING_HOMEWORK:216 names
   `target/eager-residue.txt`, the Maven-era path). `HarnessDisciplineTest.java:133` and `ParserBoundaryArchTest.java:90`
   (core tests) read the probe's SOURCE, not its output. The commit `0ad319cfd` "Proof" cites one run (9,942 bodies,
   1,620 fail).

9. **Determinism.** Not byte-reproducible: the totals line carries `build=…ms typeAll=…ms` timings
   (`EagerCorpusCompileProbe.java:99-100`). Otherwise sorted maps; residue list follows `walls` order (OPEN: whether
   `compileAllBodies` returns an ordered map).

10. **Cost.** JVM `-Xmx4096m` (`memory_mb = 4096`, `spec/BUILD.bazel:525`), resource set 4 GB. Builds the full corpus
    model and types ~9-10k bodies (1.3 s typing in 2026-09; the build dominates). No database expected (see 5).

11. **What reruns it today.** Either tree; any file under core/src or spec/src (main, test, resources, docs excluded
    only by `**/*.md` in core); any core jar; spec_tests_lib; the pools. Surprising: a core test edit (query in 5).

12. **What SHOULD rerun it.** A human asking for the measurement (after a typer change or a bump). If the user ever
    gates it, then our compiler/typer and the bump.

13. **Who runs it today.** By hand only: manual (not in `//...`), no CI lane, not in `//gates:local` (only its classpath
    is analyzed by the guards), not in Bump.java. Doc: `spec/BUILD.bazel:514-516` ("Manual: `bazel build
    //spec:eager_corpus_compile`").

14. **Recommendation: DRAFT-MANUAL** (a report a human runs on purpose; no verdict; no reader). The design's group E
    ("corpus-lane steps tagged corpus", BUILD_REBUILD_DESIGN:221) would need a pass/fail rule the user has not chosen
    (USER 2026-09-09: ungated). Keep `manual`; no writer; no diff test. Narrowing: drop `_INPUTS` (`:srcs`,
    `//core:srcs`) and the pure tree from the world-1 action; move the timings to run.log so the report is
    reproducible; depend on a corpus library split from spec_tests_lib.

15. **Open questions.** Does the user want a gate on the residue count (settle: user decision)? Does anything open a
    database during `new MinimalCorpus()` (settle: read PureTestRunner's constructor)?

---

## //spec:eager_corpus_compile_world2

1. **Identity.** Same comprehension, `spec/BUILD.bazel:517-533` (name at `:532`), testonly, manual, mnemonic Measure,
   extra flag `-Deager.world2=1`. Program and entry as above; the world-2 branch is
   `EagerCorpusCompileProbe.java:111-175`.

2. **What it computes.** World 1 as above, then a second world: the corpus plus every `.pure` file of legend-pure's
   platform roots (`SpecBodyCensusTest.PLATFORM_ROOTS` = `UpstreamFiles.PLATFORM_ROOTS`), built tolerantly (up to 400
   rounds, dropping a source that throws a ModelException), every body typed again; reports world-2 failures by reason,
   failures closed from world 1, top unknown functions, NEW failures by reason/package/type/message, and world walls.

3. **Why it exists.** Batch 169 (GATES.md:445): "A second world (corpus + legend-pure's platform packages whole) closed
   111 but poisoned 523 elements" — the evidence that moved the platform's bodied functions into the prelude. Same
   commits as above (`756a63666`, `0ad319cfd`). The question it answered is settled (the prelude now carries those
   functions); its reason is largely gone.

4. **Inputs, declared.** Identical to `:eager_corpus_compile` (aq, 17,190) plus the flag.

5. **Inputs, actually read.** World 1's reads plus the legend-pure tree via `-Dlegend.pure.root` (`:113-127`, platform
   roots only, sorted). Over-declared: the 1,413 repo source files (as above). Under-declared: none.

6. **Outputs.** Same three paths under `eager_corpus_compile_world2/`; `eager-corpus.txt` gains the `# WORLD 2` lines
   (`:151-173`), `eager-residue.txt` is world 1's residue, `run.log` the console.

7. **Committed?** No; no build consumer (same rdeps result).

8. **Who consumes it.** Nobody (grep as above); GATES.md:445 cites the 2026-09-09 numbers.

9. **Determinism.** Not reproducible (the same timing line; the 400-round drop loop is order-deterministic given sorted
   inputs).

10. **Cost.** `-Xmx4096m`; two full world builds plus up to 400 parse/build rounds — the heaviest of the four. No
    database expected.

11. **What reruns it today.** Same closure as `:eager_corpus_compile`.

12. **What SHOULD rerun it.** Only a human asking the world-2 question again.

13. **Who runs it today.** By hand only (manual; no lane; not local; not bump). Doc `spec/BUILD.bazel:515-516`.

14. **Recommendation: DRAFT-MANUAL**, and a DEAD candidate: no reader, and the question it measured was answered in
    batch 169. Keep `manual`, no writer, no diff test; or delete it (and the `-Deager.world2` branch) if the user agrees.
    If kept: same narrowing as world 1, keeping the pure tree.

15. **Open questions.** Is world 2 still wanted (settle: user)? Is it still green to build (settle: one manual run; not
    run since `0ad319cfd`'s proof, which names world 1's numbers only — OPEN whether world 2 was built then).


---

## Summary table (all 12 G1 generators)

| label | recommendation | manual? | update group | true trigger |
|---|---|---|---|---|
| //spec:gen_natives | COMMITTED-UPSTREAM | no (diff test everyday) | chain group (//core:update_generated, run by //:update_generated AND the bump) | upstream bump; hand edits to native-membership.tsv / Pure.java hand parts; our parser+resolver |
| //spec:gen_imports | COMMITTED-UPSTREAM | no | chain group | upstream bump (CompileContext.java); edits to NameResolver.java |
| //spec:gen_dynafn | COMMITTED-UPSTREAM | no | chain group | upstream bump (relationalStore registries); hand Resolution/Lite edits to DynaFn.java; chain's engine-handlers.tsv |
| //spec:gen_engine_handlers | COMMITTED-UPSTREAM (its spliced inputs are the chain files Pure.java + prelude.pure) | no | chain group | upstream bump (Handlers.java); chain's Pure.java and prelude.pure; Pure.LITE_SURFACE |
| //spec:gen_prelude | COMMITTED-SOURCE (plus bump) | no | chain group | upstream bump; FQNs our core Java names; Pure.java hand shapes; SystemMetamodel; CoreFn; claimed names; CORE_IMPORTS |
| //spec:gen_claims (+ core_next, core_next_prelude, claims_generator_lib) | DRAFT-MANUAL if D2 keeps it; DEAD if D2 retires it (no data reader) | yes | neither (uncommit; delete core_next and update_generated_4) | human request (review of the implemented surface) |
| //spec:native_declarations | DRAFT-MANUAL | yes (today: no, built by //...) | none (build output) | human request (re-keying / provenance) |
| //spec:native_membership_draft (+ //core:draft_native_membership) | DRAFT-MANUAL | yes, both (today: neither) | none (draft writer, no diff test, as today) | human request |
| //spec:ratchets | COMMITTED-SOURCE (mixed: catalog/registrations/DynaFn/test constants + bump) | yes (diff test pulls it) | own deliberate writer + explicit bump step; out of //:update_generated | our catalog, registrations, DynaFn.java, spec path constants; upstream bump |
| //spec:reference_lane_report (+ //tools/reference:ref_dump) | TEST-IN-DISGUISE | yes (already) | neither; deliberate re-bless only | our front end (parser/resolver/typer); upstream bump |
| //spec:eager_corpus_compile | DRAFT-MANUAL | yes (already) | none | human request |
| //spec:eager_corpus_compile_world2 | DRAFT-MANUAL (DEAD candidate) | yes (already) | none | human request |

Cross-cutting findings:
- **One update run is not a fixed point (0.4).** Up to 4 runs can be needed, because links read the committed copies of
  upstream links through compiled `//core`. The bump runs one update; only the diff tests catch the remainder.
- **native-claims.tsv has no reader except its own diff test (0.5).** `ClaimRegistryTest.RESOURCE` is unused. The
  claims-related corrections to the design doc, area 1 and code-traps.md are listed in 0.5.
- **Area 2 is wrong that gen_dynafn has no core dependency.** gen_dynafn reads the committed engine-handlers.tsv through
  `EngineHandlers`.
- **The diff tests' failure message is wrong for five of the six chain files.** Their outputs also move on our own edits,
  not only on the pinned upstream release.
