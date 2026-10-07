# Audit: build rebuild Phase 1, generator hygiene (`7950fafb2..3b467304d`, 14 commits)

Auditor: independent, read-only (2026-10-06). Worktree `runs/build-rebuild`, branch `build/phase1-generators`.

## Verdict: READY AFTER FIXES

No blockers. 4 should-fix, 13 nits, 3 open items for the throwaway CI.

Nothing found reads an input it does not declare. The harvest class path holds the same jars. No non-testonly target
depends on a testonly one, manual targets included. The compile-only guard gains no new (kind, mnemonic) pair. Every
decision already taken is implemented as decided. The should-fix items are three commit-message claims the code does
not support, and one visibility entry that leaves the narrowing unenforced. Each is a few lines.

## What I ran (no builds, no tests)

- `git show` / `git diff` of every commit. I read every changed file at HEAD.
- `bazel build --nobuild --keep_going` over `//...` (1,031 targets), and over **all 792 manual targets** by explicit
  label (`--target_pattern_file`), both with and without `--config=bazel10`. All four runs pass. `--config=bazel10`
  includes `--incompatible_check_testonly_for_output_files`.
- `bazel query` / `cquery` / `aquery`:
  - reverse dependencies and somepaths;
  - the per-file writer names;
  - the closure counts;
  - each changed generator's action inputs, from `aquery --output=jsonproto`.
- The fixture harvest's class path before and after step 12. I rebuilt it from each component's
  `transitive_runtime_jars` with rules_java's own merge rule: preorder, own jar, then exports+deps, then
  runtime_deps (`rules_java+/java/private/java_info.bzl:485-488`). The model reproduces HEAD's `:harvest_lib` and
  `:harvest_shims` exactly. I then compared, jar by jar:
  - every duplicated class or resource and which jar wins;
  - every `META-INF/services` file's provider order.
- An AST walk of the stress generators' import closures, including function-local imports and
  `importlib`/`__import__`/`exec` calls.
- The author's evidence, compared with the outputs in `bazel-bin`:
  - the differential tree against `diff_before` (`diff -r`, 181 files);
  - `pmcd-worklist.txt` against `pmcd_old_report.txt` (`cmp`);
  - the PAR against `par1.par` (`cmp`, plus the entry headers);
  - `pmcd-reachable.tsv` against `pmcd1.tsv`;
  - file mtimes, to confirm the second run re-executed;
  - `p1_core_gate.log` (290 pass), `pe_tests.log` (gate 8 pass), `pct_duckdb.log` (5 suites pass),
    `tests_native.log`.

Scratch scripts and outputs are in `runs/homework/phase1/audit_scratch/`.

## Answers

### 1. Under-declaration: none found

| Generator | Evidence |
|---|---|
| `gen_fixtures` (engine tree only) | **Action inputs:** 13,429. Engine tree 12,900; **pure tree 0**; `libpe_tests_lib.jar` 0; core jars: `libdiagnostics.jar` only. **No pure root:** `program_jvm_flags(..., pure = False)` sets no `-Dlegend.pure.root`, so a pure read would throw in `ProgramPaths.file` rather than pass. **Tier 2:** compiles the engine tree's sources against `java.class.path`; java_run passes `-cp` explicitly (`tools/java_run/defs.bzl:99-104`), so that is the real list on every platform. **Unsandboxed:** a run sees extra files only by path, and the harvest names no pure path. |
| `gen_manifest` (`:pe_corpus`) | **Inputs:** both trees, the harvest's output, and the pins; no `pe_tests_lib`; core jars: diagnostics only. **No class-path reads:** `Corpus`, `InlineSnippets`, `ManifestGenerator`, `ModuleFiles` and `OraclePins` contain no `getResource`/ClassLoader read, so losing `pe_tests_lib`'s 268 resources cannot change the output. |
| `gen_differential` (`stress_dense` + `stress_sources`) | **Declared files only:** `model.py` reads only `DECLARED` (`--inputs`, `model.py:62-78`). **Same set as before:** `stress_sources` ∪ `stress_dense` equals `stress_files` minus the 7 "stress" files, and `stress_sources()` drops exactly those (`GENERATED`, `model.py:90-98`). **Layout:** the committed layout is read, since `_GATE_ENV` sets no `STRESS_LAYOUT`, and it is declared. **Output:** the tree is identical to `diff_before` (`diff -r`, 181 files). |
| `gen_dense` / `gen_stress` / differential, module closures | **Exact closures:** the AST closure of `build.py` / `dense_build.py` / `differential.py` is exactly `_BUILD_MODULES` (27), `_DENSE_MODULES` (14) and `_DIFFERENTIAL_MODULES` (16). That includes function-local imports, e.g. `build.py:138-355`. **Dynamic imports:** stdlib only (`oracle.py`'s `__import__('json'/'base64'/'uuid'/'getpass')`). **`queries.pure`:** only `query.load()` reads it (`query.py:398`). That is reached from `build.py`, `differential.py`, `executed.py` and `oracle.py`'s `__main__`, never from `dense_build`'s closure. Neither the old nor the new py libraries carry data. |
| `vocab` (`token_dump`, without core) | **Inputs:** no core jar. **Mechanism:** TokenDump scans the real `-cp`. Our jars contributed nothing: `com.legend.lexer.Lexer` has no `VOCABULARY` field and returns before any entry is added. |
| keyword tool without vocab | **No core:** `somepath(//scripts/parser:keyword_coverage, //core:core)` is empty, and so is `somepath(//tools/engine-runner:vocab, //core:core)`. |
| catalog / offer / type facts / zone | **Compiles and counts:** strict deps compile. Closures: 3 / 12 / 24 / 13 / 15 first-party libraries, as claimed; `//core:core` is 33. **No lost providers:** core main registers no `META-INF/services` (there is no `core/src/main/resources/META-INF`), so narrowing cannot drop a provider. **DuckDB:** `//core:core` never carried `@duckdb_jdbc` 1.4.4 (`filter("duckdb", deps(//core:core))` is empty), so `catalog_corpus` ran on the warehouse 1.5.x jar before and after. |
| `reachability_metadata` | **Inputs:** 125, no core. **Reads:** no resource; it does not read its own JSON. |
| `gen_dynafn` (engine tree only) | **What it reads:** `Files.walk(engineRoot)` only (`DynaFnGenerator.java:66-68`), with no jvm flags. **Inputs:** pure tree 0. Core jars are still 31 (the Phase 2 self-input, as planned). |
| `gen_imports` (CompileContext only) | **What it reads:** `args[0]` only. **Inputs:** 120 (the JDK, one engine file, the `imports_generator` jar), no core. |

### 2. The fixture harvest's class path, before (`aca74e927`) and after

- **Jars:** the same **403 external jars** before and after. Our jars drop from 36 to 6:
  `libharvest_shims`, `libpe_corpus`, `libdiagnostics`, `libbase`, `libtesting`, `libharvest_program`.
- **Shims and tests-jars:** `libharvest_shims.jar` is still first. The grammar tests-jar (index 406) still precedes the
  compiler tests-jar (407), then json-unit, as before (436/437).
- **Relative order: not identical.** A longest-common-subsequence comparison shows 11 jars moved:
  - `jackson-core`, `antlr4-runtime-4.8-1`, `junit-4.13.1`, `hamcrest-core`, `commons-lang3`;
  - `legend-engine-language-pure-compiler-4.145.0`;
  - `junit-jupiter-api`, `apiguardian`, `junit-platform-commons`, `opentest4j`, `junit-jupiter-params`.
- **Why they moved:** `:harvest_shims` now reaches `:pe_corpus`, which brings jackson-databind first. Its own dep list
  then puts antlr, eclipse-collections and junit ahead of the compiler closure. `_JUNIT5` now follows the extension
  grammars.
- **ServiceLoader-visible difference: none.**
  - No class has a different winning jar. Only `META-INF/LICENSE`, `LICENSE.md`, `NOTICE` and `module-info.class`
    change winner, which is inert on a class path.
  - No `META-INF/services/*` file has a different provider order.
  - The dropped jars (`pe_tests_lib`, `builtin`, the rest of core) registered no service.
  - So tier 1's class loading, tier 2's `javac -cp` and every extension lookup see the same classes.
  - This matches the byte-identical snapshot.
- **`pe_tests_lib` (gate 8 and others):** the same check on its class path gives the same set plus `libpe_corpus.jar`,
  and no class or service difference.

### 3. Cross-platform

**`PmcdReachability`**
- **The jar list:** exec paths (`tools/jars/defs.bzl:38`, relative, `/`-separated); `Path.of` reads them on Windows.
- **The filter:** `contains("legend-engine")` matches 289 of the 396 listed jars, all of them engine artifacts by file
  name. No prefix component contains that string on any platform
  (`bazel-out/<cfg>/bin/external/rules_jvm_external++maven+maven_upstream/...`).
- **Proven pattern:** gen_fixtures' harvest list already uses the same mechanism on Windows CI.
- **Failures are loud:** a jar that fails to open or load is counted in the header, so a platform difference fails the
  diff test rather than passing silently.

**`ParGenerator.fixEntryTimes`**
- **Windows lock:** the `ZipFile` is closed before the rewrite, which matters on Windows.
- **Entry time:** `setTimeLocal(2010-01-01)` stores the DOS time only. For years 1980 to 2107 the JDK leaves `mtime`
  null, so there is no zone and no extended-timestamp field.
- **Manifest:** stays first, with `0xCAFE`.
- **Verified on the built PAR:** 6 entries, all 2010-01-01 00:00, DEFLATED; the manifest's extra field is `feca0000`.
  It is byte-identical to `par1.par`, and the `bazel-bin` copy is 84 s newer, so the second build re-executed.
- **Across platforms:** deflate bytes come from each JDK's zlib, so the PAR is deterministic per platform, not
  identical across platforms. That is fine, because the PAR is not committed.
- **Linux/Windows content determinism: unproven** (open item O1).

**`CoreImports.java` and `pmcd-reachable.tsv`**
- Both are written with `'\n'` only, and the committed copies contain 0 CR.
- `.gitattributes` (`* text=auto eol=lf`) keeps Windows checkouts LF.

**Platform line separators**
- None in a committed-output writer touched here: CatalogRulesFacts, CatalogFacts, ImportsGenerator, PmcdReachability,
  ZoneMain, TokenDump.
- `PmcdWorklist` (`println`) and `EagerCorpusCompileProbe` (`Files.write(lines)`) do use one, but both are manual
  reports that nothing commits.

### 4. testonly, manual, visibility

- **testonly:** no non-testonly target depends on a testonly one. Analysis passes over `//...` and over every manual
  target, with and without `--config=bazel10`.
- **Lanes and roots:** no CI lane, `//gates:local`, `//:generated`, `//:update_generated` or `tools/bump` names a newly
  manual or deleted target (grep of `.github`, `gates/`, the root `BUILD.bazel` and `tools/bump`).
- **Reverse dependencies of the new manual targets:**
  - The only non-manual ones are the `guard_classpaths` reports, then `classpath_test` and `//gates:local`.
  - These are analysis-only: `classpath.bzl` writes with `ctx.actions.write`. So `bazel build //...` analyzes the
    manual generators but never runs them.
  - Of the targets named like them, only `pmcd_worklist_lib` is non-manual, and it is a library.
- **New record wiring:**
  - `tests(//:generated)` lists `//parser-equivalence:update_generated_{0,1,2}_test`.
  - The checks lane (`gates-run.yml:51`) and `//gates:local` run `//:generated`.
  - The writer is under `//:update_generated` through the package umbrella (`BUILD.bazel:117`).
  - The new record is writer **0**, not 2 (should-fix S1).
- **Core visibility:**
  - The 10 added lines each name only their consuming package, as `core/BUILD.bazel:59-60` sanctions.
  - The layering tests compare deps, not visibility, and no core dep changed.
  - But the umbrella still names packages that no longer use it (S4).
  - `//spec:imports_generator` is visible to `//core` for no consumer (N3).

### 5. The compile-only guard

- **The new library:** `:native_image_metadata` registers `java_library Javac` (an empty class jar),
  `java_library JavaResourceJar` and `java_library JavaSourceJar` (aquery, macOS).
- **Allowlisted:** all three are in `JAVA_LIBRARY` (`CompileOnlyTest.java:38-42`), which the native tier includes
  (`:57`).
- **Other platforms:** these are rules_java Starlark actions, with the same rule kind and mnemonics on Linux and
  Windows. There is no new pair on any platform.
- **Other build-target changes:**
  - `//:java`'s warehouse jar loses the `META-INF/native-image` JSON.
  - `//core:compiler` gains `CoreImports.java`.
  - The wasm module is unchanged: `ZoneMain` is not in `:boundary`.
  - `CompileOnlyTest.java` is not modified in this range.

### 6. Commit messages: claims checked

**Hold**
- **Steps 2, 3, 5, 6, 9, 10, 11, 14:** every measurable claim holds, with the counts confirmed by query:
  - library closures 3 / 12 / 24 / 13 / 15;
  - 783 classes, and the worklist byte-equal to the old report;
  - CoreImports is writer `_2`, in the sorted `_GENERATED`;
  - catalog-facts.ts changes only its header.
- **"Pass as cache hits"** is valid evidence of identical bytes: the test's action key includes the generated file.

**Do not hold (S1, S2, S3)**
- **Step 7:** "keywords.py read it only in version_skew(), which nothing calls" is false (S2).
- **Step 12:** "...as before, in the same order" is false in the letter, though benign (S3).
- **Step 13:** "the third entry of //parser-equivalence:update_generated" is false (S1).

**Loose (N5, N6)**
- **Step 8:** "The gates keep :corpus, all 29: they measure with every module" (N5).
- **Step 1:** "count of the probe's sorted report lines". The test counts sort/collection sites; 8 is right (N6).

### 7. Other

The other items a strict reviewer would raise are the Nits below: dead code, stale comments, an unused visibility, a
missing nullaway reason, the remaining offer self-input, and duplicated Starlark lists. No Bazel 10 incompatibility
was found. Each step's BUILD changes are self-contained in commit order (bisectable by inspection).

All six decisions already taken are implemented as decided:
- **ref_imports deleted:** RefImports.java and both targets are gone, and the README is updated.
- **Catalog split, option B:** `catalog_rules_main` runs on `//core:sql_dialect` alone.
- **Gate 8's ledger draft:** manual, both the generator and its writer.
- **`offer_queries`:** not testonly.
- **Zone check:** `ZoneMain.answer` is text-identical to `Wasm.zoneProbe:405-411`. `zone_test`'s `Not/AZone` case
  exercises the catch.
- **The constant:** `CoreImports.SEQUENCE`.

## Findings

### Blocker

None.

### Should-fix

**S1. Step 13 renumbers parser-equivalence's writers. Its message says "the third entry."**
- **The code:** `parser-equivalence/BUILD.bazel:498-502` puts `"pmcd-reachable.tsv"` first in `files`.
  `write_source_files` names the writers by insertion order (`bazel_lib+/lib/write_source_files.bzl:178-185`). So now:
  - `update_generated_0` = pmcd (new);
  - `_1` = corpus-manifest (was `_0`);
  - `_2` = engine-grammar-fixtures (was `_1`).
  `bazel query ... --output=build` confirms this.
- **Impact:** in-repo wiring uses the umbrella and the suite, so nothing breaks. But
  `bazel run //parser-equivalence:update_generated_1` now rewrites the manifest instead of the fixture snapshot. The
  plan's tables (PHASE1_CHANGES §6) and the commit message both say the new record is writer `_2`.
- **Fix:** list the pmcd entry last, so the existing indices keep their meaning. Or keep the order and say so in the
  message: "the first entry; the manifest and fixture writers become `_1` and `_2`."

**S2. Step 7 deleted `version_skew()` and `runner_vocabulary()`, but `scripts/parser/fixtures.py` still calls them.**
- **The calls:** `fixtures.py:276-277` calls `K.version_skew()` and `K.runner_vocabulary()`, and uses the result at
  `:287` and `:304`. Both functions were deleted from `keywords.py` in a9f865aeb.
- **The message is wrong:** it says "which nothing calls."
- **Scope:** fixtures.py is a history script. `scripts/parser/README.md:28` and `HANDOFF.md:70` still document
  `python3 fixtures.py`, and it already needs a Maven-built runner (`run_parser`), so CI is unaffected.
- **Beyond the plan:** the deletion went further than PHASE1_CHANGES §5.3, which only dropped the data dependency.
- **Stale docs:** `scripts/parser/README.md:135-138` and `scripts/parser/BUILD.bazel:69` still describe the removed
  check.
- **Fix:** drop the skew lines from fixtures.py, or delete it as history; fix the two docs; correct the message.

**S3. Step 12: "in the same order" is not what the code does.**
- **What holds:** the jar set, the shims first, and grammar-before-compiler tests-jars (answer 2).
- **What does not:** 11 jars changed relative order (listed in answer 2).
- **Impact:** none observable. No winning class or service order changed.
- **Fix:** reword: "the same jars; shims first and grammar before compiler tests-jars as before; some library jars
  moved, with no duplicate class or service registration changing winner."

**S4. `//core:core`'s visibility still names `//datacube` and `//engine-client`.**
- **Where:** `core/BUILD.bazel:249`.
- **Why it matters:** since steps 5 and 9 neither package uses the umbrella. `rdeps(//..., //core:core, 1)` has no
  target in either. Left as is, a future target there can silently go back to all of core, which is exactly what this
  phase removed. Removing the two entries makes Bazel enforce the narrowing (the policy at `core/BUILD.bazel:59-60`).
- **Pre-existing:** `//wasm:__pkg__` there was already unused before this branch; it can go too.

### Nits

**N1. Dead code from step 1.**
- `EagerCorpusCompileProbe.java:6`: `import java.nio.file.Path` is unused.
- `:25-30`: `slash()` is unused (world 2 was its only caller).

**N2. Stale comments and docs.**
- `spec/BUILD.bazel:343`: "NameResolver.java's CORE_IMPORTS". It should say CoreImports.java.
- `spec/BUILD.bazel:73`: "it reads two texts". It reads one now.
- `CoreImportsParityTest.java:74`: `@DisplayName("CORE_IMPORTS is …")`.
- `scripts/corpus/BUILD.bazel:14`: calls `:corpus` "build.py's import closure plus the dense generators". It is now
  the gates' library.
- `parser-equivalence/BUILD.bazel:151`: "THE ENGINE JARS the protocol roster, PMCD reachability … read". PMCD now reads
  `:pmcd_jars_exec`.
- `parser-equivalence/BUILD.bazel:309-312`: the harvest_lib comment omits the program and the engine lists now on it.
- `core/BUILD.bazel:74-75`: the comment over `protocol` says "catalog generators" and "these". But the four targets it
  means are not adjacent, and offer-facts also reads `protocol` since step 9.
- `core/src/test/java/com/legend/compiler/SectionImportScopeKnownDefectTest.java:18`: cites `NameResolver.java:258`,
  which is now `:217`.
- `docs/BUILD_REBUILD_DESIGN_2026_10_05.md:182`: still names `ref_imports`.

**N3. `//spec:imports_generator` has an unused visibility.**
- **Where:** `spec/BUILD.bazel:79`, `visibility = ["//core:__pkg__"]`.
- **Why it is unused:** nothing in core names this library; core uses `//spec:gen_imports`.
- **Fix:** make it private.

**N4. `token_dump` sets `nullaway = False` with no reason.**
- **Where:** `tools/engine-runner/BUILD.bazel:54`.
- **Why it matters:** the macro's contract (`tools/java/defs.bzl:27`) is "its BUILD file says why", and every other
  such target in the branch carries the P1-20 comment.

**N5. Step 8's claim that the gates use every module.**
- The five gate scripts' closures are 12 to 24 modules.
- None imports deepstack, dense_mapping, dense_store, emit or functest.
- Reword, or narrow the gates in Phase 8.

**N6. Step 1's wording.**
- HarnessDisciplineTest counts sort/collection sites (its `SITE` regex), not report lines.
- I counted 8 matches, so the new pin is right.

**N7. GATES.md does not list the new record.**
- `docs/GATES.md:43-46`, the list of committed generated files, does not name `pmcd-reachable.tsv`.
- Step 14 edited that very line.

**N8. Bump.java's NEXT list.**
- `tools/bump/Bump.java:162-166` does not name `CoreImports.java` or `pmcd-reachable.tsv`.
- The record exists so an upgrade shows what moved. Phase 7 rewrites this list; optional now.

**N9. A remaining self-input is not recorded.**
- **The loop:** `emit_offer_queries` runs with `:src` (`datacube/BUILD.bazel:279-281`). `:src` includes the
  committed `src/generated/offer-facts.ts`, so `offer_facts`' chain still reads its own output. Early cutoff keeps it
  cheap.
- **Kept on purpose:** PHASE1_CHANGES §5.2 kept it deliberately.
- **Fix:** add it to the program's Phase 1 "Deferred" list, which today names only the engine-tree subsets, the parity
  test data and the PAR test.

**N10. `//spec:eager_corpus_compile` now over-declares the pure tree.**
- It still declares `UPSTREAM_TREES` and `program_jvm_flags("spec")` with the pure root.
- No `rcorpus` code reads `legend.pure.root` any more (grep); world 2 was the reader.
- It is a set-aside measurement, so narrow it with that group.

**N11. Duplicated lists in `tools/generators/defs.bzl`.**
- `ENGINE_TREE` and `ENGINE_ROOT` duplicate the engine halves of `UPSTREAM_TREES` and `UPSTREAM_ROOTS` (`:10-30`).
- Define `UPSTREAM_* = ENGINE_* + PURE_*` so the lists cannot drift.

**N12. The link dictionary tool lost its runtime check.**
- `//datacube:link_dictionary_next` is manual now, so the build no longer runs `makeDictionary` at all.
- `typecheck_test` covers types only, so a runtime throw stays unseen until a version is cut. PHASE1_CHANGES §2 noted
  this.
- A small test would restore it, later.

**N13. An allowlist entry removed without a dated note.**
- `ParserBoundaryArchTest`'s entry was removed with no dated note in the file.
- AGENTS.md says an allowlist entry moves only with a dated justification. The commit explains it; a one-line note is
  optional.

### Open items before merge (process, not code)

**O1. Linux and Windows have not run any of this.**
- Not yet run there:
  - the new pmcd generator and its diff test;
  - CoreImports' diff test;
  - every narrowed generator.
- The planned throwaway CI (every lane, three platforms) is the proof.
- The PAR's run-to-run determinism off macOS has no check at all; the permanent test is deferred.

**O2. The heavy lanes predate step 14.**
- `pe_tests.log` and `pct_duckdb.log` predate step 14's core edit, and corpus lanes 4 and 5 were not run locally.
- The edit preserves semantics (the same 32 strings, in the same order, at the same uses).
- The all-lanes CI covers it.

**O3. Expect an IN_FLIGHT conflict on rebase.**
- The branch's `docs/IN_FLIGHT.md` carries the older Phase 1 note.
- Main's full-scope note (`d18e83f3e`) supersedes it; take main's when rebasing.
