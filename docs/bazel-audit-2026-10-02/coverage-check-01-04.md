# Coverage check: does the workplan cover audit reports 01–04?

**Checked:** `docs/BAZEL_FIRST_CLASS_WORKPLAN_2026_10_03.md` (read in full, every item) against `01-core-tests.md`, `02-spec-pct-parser-equivalence-tools.md`, `03-warehouse-wasm-core-main.md` and `04-datacube-protocol-store.md` (each read in full). The audit-level plan and spike S2 were read for context. Where a verdict depends on what the code does today, I checked it read-only against the tree at the workplan's base. No build was run.

**Method.** Each finding was split into its sub-points: each bullet, each listed file group, each "also" clause, and each part of its proposed fix. A weak proof also counts as a sub-point when the finding is about a regression a test must catch. Each sub-point got one verdict:

- **COVERED:** a named item's Change fixes it, and its Proof or Done-when would show it.
- **PARTIAL:** an item addresses it but leaves part undone, or its proof would not catch a regression.
- **MISSING:** no item addresses it.
- **DEFERRED-OK:** a user decision or §6.3 records the deferral, with a reason.

Citation is not coverage. Every finding ID below is cited somewhere in the workplan. The gaps are in what the items actually change.

---

## 1. Summary

### 1.1 Sub-point verdicts

| Report | Sub-points | COVERED | PARTIAL | MISSING | DEFERRED-OK |
|---|---|---|---|---|---|
| 01 core tests (CT) | 78 | 62 | 15 | 1 | 0 |
| 02 spec, pct, parser-equivalence, tools (SP) | 78 | 56 | 16 | 4 | 2 |
| 03 warehouse, wasm, core main (WH) | 44 | 35 | 8 | 0 | 1 |
| 04 datacube, protocol, store (DC) | 53 | 46 | 3 | 4 | 0 |
| **Total** | **253** | **199** | **42** | **9** | **3** |

### 1.2 Finding-level verdicts (the worst sub-point decides)

| Report | Findings (N + re-confirmed K) | Fully covered | Some sub-point PARTIAL | Some sub-point MISSING |
|---|---|---|---|---|
| 01 | 25 | 13 | 11 | 1 |
| 02 | 33 | 21 | 9 | 3 |
| 03 | 24 | 17 | 7 | 0 |
| 04 | 16 (plus N12–N14, informational, no action asked) | 10 | 2 | 4 |
| **Total** | **98** | **61** | **29** | **8** |

### 1.3 What the gaps have in common

1. **The runfiles close-out (P3-32) is internally inconsistent.** It keeps `Repo.out`, so `testing/src/main/java/com/legend/testing/Repo.java` survives. It deletes `Repo.listed`, but no item migrates that method's 4 callers or the `java_jars` list format it reads. It deletes `Upstream.java` and says "its users moved to `Runfile` in P1-04", but P1-04 keeps `Upstream` and only changes how it resolves paths. It deletes the build-action branch of `Repo` (`-Dlegend.repo.root`), but four `java_run` generators still depend on it, and no item converts them.
2. **The walker conversion rule stops at core.** P1-05 leaves every directory walker to P3-27, and P3-27 converts only core's 29. The pe and pct walkers, and the spec and pe walkers over the upstream trees, have no item. Yet P3-32's Windows manifest-only lane needs all of them converted.
3. **The census pipeline is still orchestrated by hand.** The census lanes become tests with `env` (P3-13), and P7-14 then removes the product's env reads those lanes rely on. `RenderCensus` becomes a `bazel run` binary, but its input is undefined, and a test's outputs cannot feed a rule.
4. **Several proofs could not catch the regression they guard.** The weakest are P0-10 (tmpdir), P1-16 (DuckDB extraction), P7-14 (product env reads), P7-10/G9 (Maven-era references in source files) and P1-19 (the unlisted-platform behaviour that WH-N16 left unconfirmed).
5. **Fallbacks are not pre-registered as deferrals.** P2-09's fallback and P2-11's "recorded reason" let a hand-recorded or unpinned artifact survive, and neither fallback appears in §6.3.

---

## 2. Every PARTIAL and MISSING sub-point

The *Fix* column names a proposed amendment or new item. Each one is specified with Change, Proof and Done when in §2.2.

### 2.1 The gap table

| # | Rpt | Finding | Sub-point | Verdict | What is left, and why it matters | Fix |
|---|---|---|---|---|---|---|
| 1 | 01 | CT-K12 | `Repo.out`/`outDir`/`rel` keep `Repo.java` alive | PARTIAL | P1-05 ("`Repo.out` stays") and P3-32 ("`Repo.out` stays, or moves into `Runfile`") keep the class. 29 files call `Repo.out`/`outDir` and 3 call `Repo.rel`. G15 bans only `Repo.path`/`Repo.module`, so the remnant passes every guard. **The remnant is not justified.** `TEST_UNDECLARED_OUTPUTS_DIR` is a Bazel protocol variable and needs only an honestly named five-line helper. Worse, in a build action `Repo.out` silently writes to an undeclared temporary directory (`actionScratch`). Moving it "into `Runfile`" would also be wrong: outputs are not runfiles. | A1 |
| 2 | 01 | CT-K12 | Action mode: `-Dlegend.repo.root`/`.module` | MISSING | `tools/generators/defs.bzl` `program_jvm_flags` passes `-Dlegend.repo.root=.` to `//parser-equivalence:gen_fixtures`, `gen_manifest`, `gen_roster` and `gen_own_corpus_draft`. Through `Corpus`, `RosterGenerator` and `FixtureHarvestGenerator`, these call `Repo.path`, `Repo.listed`, `Repo.out` and `Upstream`. P3-32 deletes `Repo.java:44-77`, which contains this branch. No item converts the generators, so P3-32 breaks four diff-tested generators. | A1 |
| 3 | 01 | CT-K12 | `actionScratch` side reports | PARTIAL | P0-10 gives each `java_run` a declared scratch directory, and P3-32 deletes `actionScratch`. But action-mode `Repo.out` still calls `actionScratch`, and nothing points the generators' side reports at the declared directory. After P3-32, `Repo.out` in an action would dereference a null `TEST_UNDECLARED_OUTPUTS_DIR`. | A1 |
| 4 | 02 | SP-K12 | `Repo.listed` deleted with live callers | MISSING | P3-32 deletes `listed` (`Repo.java:109-121`). Its callers are `GrammarCoverageCensusTest:650`, `PmcdReachabilityCensusTest:38`, and two action generators, `RosterGenerator:68` and `FixtureHarvestGenerator:41`. `tools/jars/defs.bzl` writes `short_path` lines (`../<repo>/…`) that only `Repo.listed`'s hand resolution understands. P3-27 even routes `NoEagerTypeReferencesTest` through `java_jars`. No item changes the list format or migrates the readers. | A1 |
| 5 | 02 | SP-K12 | `Upstream.java` deleted | PARTIAL | This is a contradiction. P1-04 keeps `Upstream.java` and swaps its internals to `Runfile.of`, leaving its 16 callers untouched. P3-32 says "Delete `Upstream.java`; its users moved to `Runfile` in P1-04". Also, P1-04's Risk line ("generators … are unaffected") is false: `FixtureHarvestGenerator:55` and `Corpus` call `Upstream.engine()` inside `java_run` actions, where the property holds an exec path, not an rlocationpath. `Runfile.of` on that path fails, so P1-04 as written breaks `gen_fixtures`, `gen_manifest` and `gen_roster`. | A1, A2 |
| 6 | 02 | SP-K12 | Directory walkers outside core | MISSING | P1-05: "a test that walks a directory … leave those for P3-27". P3-27 converts only core's 29 files. These have no item: the Repo-root walkers `parser-equivalence/…/Corpus.java`, `FixtureAdjudicationTest`, `FixtureCorpusParityTest`, `MutationFuzzTest`, `SurfaceCensusTest` and `pct/…/PctDisciplineTest`; and the upstream-tree walkers `CatalogUpstreamDiffTest`, `ManifestWorldCensusTest`, `OurResolutions`, `SpecBodyCensusTest`, `UpstreamDeclarations` and `EagerCorpusCompileProbe`. P1-05's own Done-when (only the 29 core files may still use `Repo.path`/`module`) cannot be met. P3-32's Windows manifest-only lane will fail on them, or will walk undeclared source directories. | A3 |
| 7 | 02 | SP-N21 | `PctDisciplineTest:71` walks `pct/src` | PARTIAL | It is listed in SP-N21, but neither P3-30's list nor P3-27 (core only) includes it. | A3 |
| 8 | 01 | CT-N19 | `$(rlocationpaths)` of about 740 files on the Windows command line | PARTIAL | P3-27 passes `$(rlocationpaths //core:main_java)` through `jvm_flags`. That is 742 Java files at about 60–70 characters each, over 45,000 characters, against Windows' 32,767-character `CreateProcess` limit. Bazel's Windows Java launcher puts `jvm_flags` on the command line. So the guards break on exactly the platform P3-32 makes manifest-only. | A4 |
| 9 | 01 | CT-GJ | `ArchitectureTest` code-source-path workaround | PARTIAL | P3-27 adds `:duckdb_load` to the classpath but keeps the workaround at `ArchitectureTest.java:36-104`, which derives class locations from the code source. That has the same fragility as CT-N9, which P3-27 fixes for `NoEagerTypeReferencesTest` only. | A4 |
| 10 | 01 | CT-N7 | `render`'s input is a Bazel output | PARTIAL | P3-13 makes `//tools/census:render` a `java_binary`. It does not say where render reads the recorded cases from. Today that is scraped `bazel-testlogs`, via `tools/census/lanes.sh`. A `bazel run` binary over testlogs is still non-Bazel orchestration. | A5 |
| 11 | 02 | SP-N2 | Render census as an action over declared inputs | PARTIAL | Same gap as row 10. The audit's fix ("an action over the jars of `//core:core_tests` and the recorded cases") is replaced by a hand-run binary. | A5 |
| 12 | 02 | SP-N2 | `LL_PCT_CASES` recording | PARTIAL | This is a contradiction. P3-13 builds `//tools/census:lanes` with `env = {"LEGEND_LITE_DUMP_SQL": "1", "LL_PCT_CASES": "1"}`. P7-14 replaces the product's env reads with an option object set "through their own `main`, not env". After P7-14, the P3-13 lanes would record nothing. | A5 |
| 13 | 03 | WH-N10 | Census "collected by a rule" | PARTIAL | A test's undeclared outputs cannot be an input to any rule. The census must run as actions (as P2-09(b) does for the agent recording) for a rule to collect it. Rows 10–12 are the same gap. | A5 |
| 14 | 03 | WH-N10 | Proof or guard for product env and property reads | PARTIAL | P7-14's proof greps only `getenv("LL_` and `getenv("LEGEND_LITE_` in `core/src/main`. It misses `legend.spec.trace` and `legend.mapping.trace` (`System.getProperty`), `TEST_UNDECLARED_OUTPUTS_DIR`, and every other module's `src/main`. G4 scans only test sources. A new debug switch in product code would pass every check. | A6 |
| 15 | 01 | CT-N2 | Class excluded from the gate-1 integration target | PARTIAL | P3-18 creates `//core:corpus_differential_test`. P3-05's `integration` target (`--select-package=com.legend.integration`) would still select `CorpusDifferentialTest`, now without its data. Once P3-14 turns the assumption into an assertion, gate 1 fails. | A7 |
| 16 | 01 | CT-N12 | Guard: `void test*` without a JUnit annotation | PARTIAL | The audit asks for a guard against methods like `testContainsPrimitive()`, which has no `@Test`. G13 (P6-13) checks only annotated methods (do they assert?) and G3 checks classes. An unannotated `test*` method stays invisible. | A8 |
| 17 | 01 | CT-N14 | Delta-asserting tests under parallelism | PARTIAL | P3-04 fixes the `reset()`-then-total tests only. `LiteralChannelTest`, `AssertVerdictsTest`, `InstanceIdentityTest` and `RowLoadTest` still assert before/after deltas on process-wide counters. They are safe while JUnit runs sequentially, and nothing records or enforces that. Low. | A9 |
| 18 | 01 | CT-N17 | About 25 core guardrail and census ratchet constants | PARTIAL | P2-16(b) lists the spec, pct and pe families plus "core's `GuardCoverage` floors". It does not list the core guardrail and census ratchets (`TenetRatchet`, `SqlTextRatchet`, `JavaEvalLedger`, `CarrierPurity`, …). Its proof has no `//core:update_ratchets_test`, so core can finish with hand-copied measurements. | A10 |
| 19 | 01 | CT-N18 | Stress corpus shipped twice | PARTIAL | P3-05 drops `data` only from `core_tests` and `guardrails` (`core/BUILD.bazel:321, 345`). `stress_suites` and `scale_*` keep `_STRESS_READS = _CORE_READS + ["//projects:srcs"]` (`:268, 381, 400`), and the 13 MB corpus stays both a jar resource (`:282`) and runfiles data. P3-05 says "read as classpath resources" while P3-27 converts `StressCorpus` to file lists: pick one. | A11 |
| 20 | 01 | CT-N18 | `_CENSUS_READS` includes `//projects:srcs` | PARTIAL | No item removes it (`core/BUILD.bazel:270-273`). G12 matches `glob(["src/**"])` text, not a filegroup label, so it cannot catch this. | A11 |
| 21 | 01 | CT-K15 | Census and stress lanes' coarse data | PARTIAL | This is rows 19 and 20 from the K15 side. | A11 |
| 22 | 03 | WH-K15 | Census and stress coarse data | PARTIAL | Same as row 21 (WH re-confirms K15). | A11 |
| 23 | 01 | CT-K15 | Redundant `"heavy"` exclusion | PARTIAL | Not mentioned anywhere. Low, but it is dead policy in BUILD. | A11 |
| 24 | 01 | CT-K18 | `scale_*`: manual, in no lane, timing only | PARTIAL | P3-05 fixes only the "50K" comment. The six targets assert nothing beyond "no failures" and print timings, which makes them diagnostics written as tests. No item decides between a `bazel run` benchmark and a real test, and §6.3 has no deferral. | A11 |
| 25 | 01 | CT-N8 | Proof detects a tmpdir regression | PARTIAL | P0-10's proof checks generated-file identity and runs `//json:tests` under `tr_TR`. Nothing asserts `java.io.tmpdir == $TEST_TMPDIR` inside a test, or the action scratch directory inside `java_run`. A regression in `JUnitMain`'s first line would pass silently. | A12 |
| 26 | 02 | SP-N1 | Maven `-Dtest` javadocs in kept probes | PARTIAL | P3-17 turns the probes into binaries but leaves "run with `mvn test -Dtest=…`" (`ProbeWireShapes.java:15`, `EagerCorpusCompileProbe.java:17-18`, `ParseSpeedBenchmarkTest.java:18`). P7-10 does not list them, and G9 scans only `.md` files. | A13 |
| 27 | 02 | SP-N1 | `ZFixtureAdjudicationProbe` reads an earlier run's `Repo.out` | PARTIAL | P3-17 says "become `java_binary` diagnostics" but not with declared inputs. The audit asks for declared outputs. As written, the binary would still look for `engine-fixtures.jsonl` left by another process (`:43-46`). | A13 |
| 28 | 02 | SP-N2 | `RefImports` README recipe | PARTIAL | P7-08 lists `tools/reference/README.md:29-42`. The hand `"$JB/javac" … java -cp` recipe is also at `:94-96`. G9's regex matches `java -jar` but not `javac` or `java -cp`, so the guard would not catch it either. | A14 |
| 29 | 02 | SP-N3 | Consumer `keywords.py` repaired | MISSING | D3 row 6 was decided **keep and repair**. P2-18 produces `vocab.tsv`, but no item repairs `scripts/parser/keywords.py` and `tiers.py` (the `.g4` from `@legend_engine_src`, a `py_binary` reading the generated `vocab.tsv`). P7-03's list omits them. | A15 |
| 30 | 02 | SP-N7 | `initdb` as root | PARTIAL | P3-20 only changes the failure message. P5-02's image does not declare a non-root user, so a container CI job running as root still cannot run any Postgres test. | A16 |
| 31 | 02 | SP-N7 | Orphaned `postgres` on SIGKILL | PARTIAL | `EmbeddedPostgres` starts the server with `pg_ctl` (`:108`), and `pg_ctl` calls `setsid()`, so the postmaster leaves the test's process group. P3-20's fix, an `AfterAllCallback`, does not run on SIGKILL either. P3-20's own Done-when (no survivor after `--test_timeout=1`) is correct but unreachable with that Change. | A16 |
| 32 | 02 | SP-N9 | `ChannelB*Test` cumulative counters | PARTIAL | P3-09 makes the PCT suites per-suite. `//pct:pct_channel_b` still runs every `channelb` class in one JVM, asserting cumulative `CanonicalDivergence`/`SqlTypeCensus` counts (`ChannelBEssentialTest.java:207, 244, 255`). With P1-01's `--test_filter` or a class-order change, it measures something else. | A17 |
| 33 | 02 | SP-N16 | Oracle manifests from the pinned tree | PARTIAL | P2-11's Done-when accepts "each one carries a recorded reason". That would leave snapshots of an unpinned commit (`943d38b3`) with no producer and no diff test, the exact finding. | A18 |
| 34 | 02 | SP-N19 | Locale enforcement outside core libraries | PARTIAL | The Error Prone flags go into `legend_java_library` (P1-20), which converts only `core/BUILD.bazel:34-202`. `ReplayOracle` and `H2Verify` (spec) are fixed by hand but not enforced. | A19 |
| 35 | 03 | WH-N8 | Error Prone "in the shared javacopts" | PARTIAL | Same scope gap: warehouse, json, base, wasm, testing, tools and the spec/pct/pe libraries never get the flags. P3-28's Done-when ("a locale-less case mapping no longer compiles") holds only for core. | A19 |
| 36 | 02 | SP-K10 | `git ls-remote` from PATH | MISSING | `Bump.java:222-226, 258-265` run host `git`. Nothing replaces them, although `Bump` already has an `HttpClient` that could ask GitHub's API instead. | A20 |
| 37 | 02 | SP-K10 | Nested `bazel` | PARTIAL | P2-10 swaps PATH `bazel` for `$BAZEL_REAL`, but `Bump` still runs Bazel from inside `bazel run`. That is non-Bazel orchestration, neither removed nor recorded as an exception. | A20 |
| 38 | 02 | SP-K10 | Regex edits of `MODULE.bazel` | PARTIAL | The `oracle-pins.env` regex goes; the `MODULE.bazel` regex stays. P2-10 rules out a `.bzl` because MODULE.bazel cannot load one. But Bazel 9 supports `include()` of a `*.MODULE.bazel` segment, which allows a whole-file rewrite with no regex. | A20 |
| 39 | 02 | SP-K15 | Whole upstream trees; `DynaFnGenerator` | PARTIAL | P3-07 says to investigate, "narrow where yes; record the rest". It names no place for the record, and its Done-when checks only `glob(["src/**"])`. `@legend_engine_src//:tree` (12,900 files, 15 consumers) can stay as it is with P3-07 marked done. | A21 |
| 40 | 02 | SP-K18 | `corpus_warehouse` manual | PARTIAL | P3-19 runs it but says "(still manual)". R1 makes `bazel test //...` the full gate. Neither a lane nor a §6.3 deferral is recorded. | A22 |
| 41 | 03 | WH-N1 | Fallback leaves the non-foreign sections hand-recorded | PARTIAL | P2-09's fallback ships (a), the foreign section, alone, and records (b) as "deferred". That leaves the resource and reflection sections, including the host-locale bundles `FormatData_en_US` and `icudt76b`, hand-recorded with no producer. The deferral is not pre-registered in §6.3. | A23 |
| 42 | 03 | WH-N2 | Proof detects a regression | PARTIAL | P1-16's proof checks that `ls ${TMPDIR}/legend-warehouse-duckdb` does not reappear. After P0-10, `java.io.tmpdir` is `TEST_TMPDIR`, so a surviving extraction would write there and the check would still pass. | A24 |
| 43 | 03 | WH-N14 | Other Maven-era comments; no guard over sources | PARTIAL | P7-10's list covers the five sites WH-N14 names. These remain: `NoEagerTypeReferencesTest.java:33, 64`; `CorpusDifferentialTest.java:28`; `PctCensusGate.java:17`; `pct/BUILD.bazel:89`; `core/BUILD.bazel:368`; `tools/junit/defs.bzl:7`; plus the three in row 26. P7-10's proof grep has no `mvn`, `-Dtest=`, `surefire` or `target/`, and G9 is `.md`-only. | A13 |
| 44 | 03 | WH-N16 | Proof on an unlisted platform | PARTIAL | WH-N16 explicitly left open whether `target_compatible_with` short-circuits the select error. P1-19's proof (the same compatible set before and after) never analyses an unlisted platform, so the open question stays open. | A25 |
| 45 | 04 | DC-N2 | G12 vs the scanners' whole-tree data | PARTIAL | P3-29 keeps `portability` and `guardrails` scanning `glob(["tools/**", "bench/**", …])` sets, which is legitimate: they are scanners. But G12 fails on "any `glob([...**...])` passed to a test's `data`". G12 and P3-29 cannot both pass as written. | A26 |
| 46 | 04 | DC-N5 | `quiet()`/`until()` polling heuristics | MISSING | P3-16 omits `test/pivot-rows/pivot-rows.ts:143-149`, `test/typed-values/typed-values.ts:103-109` and `test/json-read/json-read.ts:125-131`. Its Done-when ("sleeps against a timer") does not clearly catch polling loops. | A27 |
| 47 | 04 | DC-N6 | `conformance.ts` run IDs from `Date.now()`/`Math.random()` | MISSING | `query-store/test/conformance.ts:42, 67`. Low. | A27 |
| 48 | 04 | DC-N8 | Global DOM mutation never restored | PARTIAL | This is safe only because each Bazel target is one process. P4-15 deletes the npm script that broke that. But no item states the one-file-per-target invariant that the safety rests on. Low. | A28 |
| 49 | 04 | DC-N10 | WASM-ness derived from deps | PARTIAL | `node_test(wasm = True)` is still a hand flag per target: the `_WASM_TESTS` list moved into a macro argument. | A28 |
| 50 | 04 | DC-N10 | `.mjs` files never typechecked | MISSING | `datacube/demo/*.mjs` (the harnesses P4 rewrites), `datacube/bench/*.mjs`, the strict reporter (moving to `tools/js/`) and `fixtures/saved-queries/make.mjs` (repaired in P2-06) are never typechecked. | A29 |
| 51 | 04 | DC-K15 | Every unit test depends on all of `:src` | MISSING | `datacube/BUILD.bazel:32`. Editing any one of about 100 source files re-runs every DataCube test. This is the JS twin of P3-06, but it has no item and no §6.3 row. | A30 |

(51 rows: the 42 PARTIAL and 9 MISSING sub-points.)

### 2.2 Proposed amendments and new items

#### A1 · NEW P3-33 · `Repo.java` and `Upstream.java` are deleted; generator actions name their inputs

- **Change:**
  - `testing/…/TestOutputs.java`: `static Path file(String first, String... more)` resolves under `$TEST_UNDECLARED_OUTPUTS_DIR`, failing if it is unset, and creates parent directories. Migrate the 29 `Repo.out`/`outDir` files.
  - Move `Repo.rel` into P3-27's `SourceFiles`.
  - `Runfile.listed(String property)` reads a list file of **rlocationpaths**. `tools/jars/defs.bzl` (`exec_paths = False`) writes `<workspace>/<short_path>` lines, with `ctx.workspace_name` for the main repository and the repository name for external jars, instead of `../<repo>/…`. Migrate `GrammarCoverageCensusTest`, `PmcdReachabilityCensusTest` and P3-27's `NoEagerTypeReferencesTest`.
  - Action mode: `FixtureHarvestGenerator`, `ManifestGenerator`, `RosterGenerator` and `OwnCorpusLedgerDraft` take each input as an explicit argument or `-D` flag carrying `java_run` `{TOKEN}`/`$(execpath)` values, read with `Path.of`. Side reports go to an explicit `--report-dir`, which is P0-10's declared scratch tree. Delete `-Dlegend.repo.root`/`-Dlegend.repo.module` from `tools/generators/defs.bzl`'s `program_jvm_flags`.
  - `Upstream`'s 16 test callers use `Runfile.dirOf("legend.engine.root")` (`Runfile.of(...).getParent()`).
  - `git rm testing/…/Repo.java testing/…/Upstream.java`.
  - Extend G15 (P6-15) to fail on `com.legend.testing.Repo`, `com.legend.testing.Upstream` and `legend.repo.root`.
  - P3-32 then depends on P3-33, and its "`Repo.out` stays" line is deleted.
- **Proof:** `git ls-files testing/src/main/java/com/legend/testing/` lists no `Repo.java` or `Upstream.java`. `git grep -n "testing\.Repo\|testing\.Upstream\|legend\.repo\.root"` is empty. `bazel test //parser-equivalence:update_generated_tests //:generated` passes: the four generators stay byte-identical. **Heavy, one at a time:** `//core:core_tests //core:census`, `//spec:spec_tests`, `//parser-equivalence:parser_parity`. `bazel test //tools/guards:runfiles_test` fails when a scratch file reintroduces `Repo.out`.
- **Done when:** both classes are gone, every generator action names its inputs, and G15 blocks their return.

#### A2 · Amend P1-04 (do not break the generator actions in transit)

- **Change:** P1-04 must not swap `Upstream`'s internals to `Runfile.of` while action-mode callers exist. Either land A1's action-mode conversion first, or keep the exec-path branch until A1. Correct the Risk line: generators *are* affected, through `Corpus` and `FixtureHarvestGenerator`.
- **Proof:** P1-04's proof plus `bazel test //parser-equivalence:update_generated_tests`.
- **Done when:** P1-04 merges with every diff test green.

#### A3 · NEW P3-27b · The walker rule applied outside core

- **Change:**
  - Convert the Repo-root walkers to `SourceFiles.of(property)` over declared lists: `parser-equivalence/…/Corpus.java`, `FixtureAdjudicationTest`, `FixtureCorpusParityTest`, `MutationFuzzTest`, `SurfaceCensusTest` and `pct/…/PctDisciplineTest`.
  - The upstream-tree walkers (`CatalogUpstreamDiffTest`, `ManifestWorldCensusTest`, `OurResolutions`, `SpecBodyCensusTest`, `UpstreamDeclarations`, `EagerCorpusCompileProbe`) take narrowed filegroups from `third_party/legend_engine_src.BUILD`/`legend_pure_src.BUILD`, shared with P3-07's narrowing.
  - P1-05's Done-when becomes "only the walker files that P3-27 **or P3-27b** own".
- **Proof:** **Heavy:** `bazel test //spec:spec_tests //parser-equivalence:parser_parity //pct:pct_channel_b`. Then `git grep -n "Files\.\(walk\|list\)" -- spec/src pct/src parser-equivalence/src` shows no walk rooted at a runfiles or upstream directory. Each remaining walk carries a dated `.allow` row in G15's allowlist, for example a walk inside a tree artifact.
- **Done when:** P3-32's Windows manifest-only lane passes on spec, pct and pe with no tree-mode reader.

#### A4 · Amend P3-27 (Windows-safe lists; ArchitectureTest)

- **Change:**
  - Replace `$(rlocationpaths …)` in `jvm_flags` with one `$(rlocationpath :<name>_files)`, where `:<name>_files` is a small `file_list` rule (or `java_jars`-style rule) that writes the rlocationpaths, one per line, and is read through `SourceFiles.of`.
  - `ArchitectureTest` imports `ClassFileImporter().importPaths(Runfile.listed("core.jars"))` from a `java_jars` list, and the code-source workaround at `:36-104` is deleted.
- **Proof:** `CI` Windows: `bazel test //core:guardrails //core:census` without `--enable_runfiles`. `git grep -n "getCodeSource" core/src/test` is empty.
- **Done when:** no `jvm_flags` value carries a file set, and no guard derives class locations from the classpath.

#### A5 · Amend P3-13 and P7-14 (the census as actions, consistent with the option object)

- **Change:**
  - The 8 census lanes become `java_run` actions that run `JUnitMain` over the lane's selection, as P2-09(b) does. Each passes `-Dlegend.diagnostics=dump-sql,pct-cases`, read once by the P7-14 `Diagnostics` object at the runner entry, so no product env is read. Each action declares its outputs, the SQL dumps and the recorded cases, as tree artifacts.
  - `//tools/census:report` is a `java_run` of `RenderCensus` over those outputs and the `//core:core_tests` jars (`java_jars`). It writes a declared report, made a `write_source_files` golden if it is committed.
  - `lanes_diff` reads two report outputs.
  - Delete `lanes.sh` and `render.sh` (after P7-01). Remove the `env` dictionary from P3-13.
- **Proof:** `bazel build //tools/census:report` (heavy as the lanes), then a second build is fully cached. `git grep -n "bazel-testlogs\|bazel info" tools/census` is empty.
- **Done when:** the census is produced by `bazel build`, with no testlog scraping and no env switch.

#### A6 · NEW P6-20 · G20: product code reads only declared environment and properties

- **Change:** `//tools/guards:product_env_test` scans `*/src/main/**` (inventory) for `System.getenv`, `System.getProperty`, `Boolean.getBoolean`, `Integer.getInteger`, `Long.getLong` and `process.env`. Allowed: a dated `.allow` list, for example `LegendHttpServer`'s `PORT` and the warehouse `Config` flags. `TEST_*` names are never allowed. Replace P7-14's narrow grep with this test.
- **Proof:** `bazel test //tools/guards:product_env_test`. Negative: add `System.getProperty("legend.x.trace")` to a scratch product file, and the test fails.
- **Done when:** a new product debug switch needs a dated allowlist row.

#### A7 · Amend P3-18 and P3-05 (one home for `CorpusDifferentialTest`)

- **Change:** P3-05's `integration` target passes `--exclude-classname=.*CorpusDifferentialTest`, or the class moves to `com.legend.integration.differential` and is excluded by package. The class is selected by exactly one target.
- **Proof:** `bazel test //core:core_tests //core:corpus_differential_test`. G3 (P6-03) is extended to flag a class selected by **two** non-manual targets.
- **Done when:** the class runs once, only where its data is declared.

#### A8 · Amend P6-13 (G13)

- **Change:** add a rule: in a test class, any `void test*()` method with no parameters and no JUnit annotation fails, unless allowlisted with a date.
- **Proof:** negative check: remove `@Test` from a scratch method, and the test fails.
- **Done when:** a test method cannot silently lose its annotation.

#### A9 · Amend P3-04 (low)

- **Change:** `@ResourceLock("process-counters")` on `LiteralChannelTest`, `AssertVerdictsTest`, `InstanceIdentityTest`, `RowLoadTest` and the P3-04 classes. Add a G13 rule: no `junit-platform.properties` enables parallel execution without a dated row.
- **Proof:** `bazel test //tools/guards:test_discipline_test`.
- **Done when:** turning on JUnit parallelism cannot silently break the delta assertions.

#### A10 · Amend P2-16 (core's ratchet families)

- **Change:** investigate first with `git grep -n "static final int [A-Z_]*\(MAX\|MIN\|CEILING\|FLOOR\|PIN\)" core/src/test`. Enumerate core's guardrail and census families in the item, as spec's are. Add `core/ratchets.tsv` and `//core:update_ratchets_test`.
- **Proof:** **Heavy: H-core:** `bazel test //core:update_ratchets_test //core:guardrails //core:census`.
- **Done when:** core has no measured value that is copied by hand.

#### A11 · Amend P3-05 (the remaining core data and the manual lanes)

- **Change:**
  - `census`, `stress_suites` and `scale_*` drop `_CENSUS_READS`/`_STRESS_READS`. Each lists what it reads (P3-27's lists), and `//projects:srcs` leaves `census`.
  - The stress corpus is read one way only: either a classpath resource (drop the runfiles data) or a declared file list (drop it from `core_tests_lib`'s resources).
  - Delete `_CORE_READS` itself, and the redundant `"heavy"` exclude-tag.
  - `scale_*` become `java_binary` benchmarks (`bazel run //core:scale -- chaotic`) or real tests with assertions. Record the choice; if they stay manual, add a §6.3 row with the reason.
  - Extend G12 to fail on any test whose `data` names a `:srcs`-style whole-package filegroup.
- **Proof:** `bazel query 'labels(data, //core:census + //core:stress_suites)'` contains no `//projects:srcs` and no whole-tree glob. `bazel test //tools/guards:data_globs_test`. **Heavy: H-stress, H-core.**
- **Done when:** no core target ships or declares data it does not read, and no core test is a manual timing script.

#### A12 · Amend P0-10's proof

- **Change:** add fixtures to `//tools/junit:runner_test` asserting `java.io.tmpdir` equals `$TEST_TMPDIR` and `Locale.getDefault()` is `en_US`. Add a `java_run` fixture (`//tools/java_run:pins_test`, a `diff_test` of an action that prints `java.io.tmpdir`'s parent's name, locale and encoding) against the declared scratch directory.
- **Proof:** `bazel test //tools/junit:runner_test //tools/java_run:pins_test`. Negative: drop the tmpdir line, and the test fails.
- **Done when:** removing any pin fails a test.

#### A13 · Amend P3-17 and P7-10 (probes and Maven-era text in sources)

- **Change:**
  - P3-17: every kept probe binary takes inputs as arguments and writes to `--out`. `ZFixtureAdjudicationProbe` takes the fixtures file as an argument, and its javadoc gives the `bazel run` line.
  - P7-10 adds `ProbeWireShapes.java:15`, `EagerCorpusCompileProbe.java:17-18`, `ParseSpeedBenchmarkTest.java:18`, `NoEagerTypeReferencesTest.java:33, 64`, `CorpusDifferentialTest.java:28`, `PctCensusGate.java:17`, `pct/BUILD.bazel:89`, `core/BUILD.bazel:368` and `tools/junit/defs.bzl:7`.
  - P7-10's proof grep adds `\bmvn\b|-Dtest=|surefire|target/classes|core/target`.
  - G9 (P6-09) gains a second scan over `*.java`, `*.bzl`, `BUILD.bazel`, `*.ts` and `*.mjs` (outside `docs/history/**` and `experiments/**`) for the same tokens.
- **Proof:** `bazel test //tools/guards:docs_test`. Negative: add `mvn test` to a scratch javadoc, and the test fails.
- **Done when:** no first-party source file names a Maven command or a `target/` path.

#### A14 · Amend P7-08 and P6-09

- **Change:** P7-08 adds `tools/reference/README.md:94-96`. G9's regex adds `\bjavac\b` and `\bjava -cp\b`.
- **Proof:** `bazel test //tools/guards:docs_test` fails on the old README text.
- **Done when:** no document gives a hand `javac`/`java -cp` recipe.

#### A15 · Amend P7-03 (D3 row 6)

- **Change:** `scripts/parser/{keywords,tiers}.py` become `py_binary`s. The `.g4` grammars come from `@legend_engine_src` as data, and `vocab.tsv` from P2-18's output. If they emit a tracked report, it is a `write_source_files` golden.
- **Proof:** `bazel run //scripts/parser:keywords -- --help`, then `bazel build //scripts/parser:all`.
- **Done when:** the decided "keep and repair" row is a working target.

#### A16 · Amend P3-20 and P5-02 (Postgres lifetime and root)

- **Change:**
  - `EmbeddedPostgres` starts `postgres -D <cluster>` directly as a child process (no `pg_ctl start`, which calls `setsid`), so Bazel's process-wrapper or sandbox kills it with the test's process group. Stop it with `SIGINT`/`destroy()`.
  - P5-02's image sets `user = "1000:1000"` (`oci_image` `user`), so CI never runs `initdb` as root.
- **Proof:** P3-20's existing Done-when (`pgrep postgres` is empty after `--test_timeout=1`), now reachable. **Heavy: H-docker:** inside the image, `bazel test //core:postgres_arm_test` passes.
- **Done when:** no timed-out test leaves a server behind, and CI's Postgres tests run in the pinned image.

#### A17 · Amend P3-09 (ChannelB)

- **Change:** `pct_channel_b` becomes one `junit_test` per `ChannelB*Test` class (as D2 (b) does for the suites), or each class resets the counters in `@BeforeAll` and asserts its own deltas. Any moved pin carries a dated justification naming P3-09.
- **Proof:** `bazel test //pct:pct_channel_b`, and `bazel test //pct:pct_channel_b --test_filter=ChannelBEssentialTest` gives the same verdicts.
- **Done when:** a ChannelB verdict does not depend on which classes share its JVM.

#### A18 · Amend P2-11

- **Change:** delete the "recorded reason" exit. If `@legend_engine_src` at the pin lacks the manifests, they are produced by an action (from the pinned tree's PCT sources, or from the engine release's `pct-manifests` artifact pulled through `@maven_runner`) and committed through `write_source_files`. If neither exists, the reason goes to §6.3 with the user's decision.
- **Proof:** `bazel test //pct:update_oracle_manifests_test //pct:pct_channel_b`.
- **Done when:** every manifest is either pinned-tree data or a diff-tested action output.

#### A19 · Amend P1-20 and P3-28 (locale enforcement everywhere)

- **Change:** `legend_java_library` is used by every first-party `java_library`, in `base`, `json`, `warehouse`, `wasm`, `testing`, `tools`, `spec`, `pct` and `parser-equivalence` as well as core, or the two `-Xep` flags go into a registered `default_java_toolchain`'s `javacopts`, with test-only libraries exempted by a `-XepDisable` list. Add a guard: a `genquery` over `kind(java_library, //...)`, checked against the macro's `generator_function`.
- **Proof:** `bazel build //...`. Negative: `"x".toUpperCase()` in a scratch warehouse file fails to compile.
- **Done when:** P3-28's Done-when holds repository-wide.

#### A20 · Amend P2-10 (Bump without host git, nested Bazel or regex)

- **Change:**
  - Replace `git ls-remote` with GitHub's `GET /repos/{owner}/{repo}/git/ref/tags/{tag}` through the existing `HttpClient`.
  - Move the release constants into `release.MODULE.bazel`, pulled in with `include()` from the root `MODULE.bazel`. Bump rewrites that whole file, with no regex.
  - Bump stops running Bazel: it prints the two repin commands, and P0-04's `--lockfile_mode=error` fails CI until they are run. If nested Bazel is kept, record it as a dated exception in §6.3.
- **Proof:** `bazel build //tools/bump`. `git grep -n "ls-remote\|ProcessBuilder" tools/bump` is empty, or allowlisted with a date.
- **Done when:** Bump needs no host tool, and edits no file by regex.

#### A21 · Amend P3-07

- **Change:** the Done-when gains a table of every `@legend_engine_src//:tree`/`@legend_pure_src//:tree` consumer: its narrowed filegroup, or a dated §6.3 row with the reason (for example `DynaFnGenerator` scans the whole tree by design).
- **Proof:** `bazel query 'rdeps(//..., @legend_engine_src//:tree)'` equals the table's "unnarrowed" rows.
- **Done when:** each whole-tree input is narrowed or justified in writing.

#### A22 · Amend P3-19 (`corpus_warehouse`)

- **Change:** decide. Either it joins `//gates:native` (non-manual; it needs the native binary), or it gets a §6.3 row with its cost and a schedule (for example nightly via `--config=ci` plus `//gates:manual_nightly`).
- **Proof:** `bazel query 'attr(tags, manual, tests(//...))'` lists only targets that have §6.3 rows.
- **Done when:** no manual test exists without a recorded reason. This also closes `scale_*` (A11) and P3-25's `engine_stress`.

#### A23 · Amend P2-09 and §6.3

- **Change:** if the fallback is taken, the non-foreign sections are produced by a deterministic generator (a `java_run` that filters host-locale resource bundles to a fixed list and emits the resource patterns), not left hand-recorded. Pre-register the fallback in §6.3. Update `docs/WAREHOUSE_W1_DESIGN_2026_09_26.md:333-338` ("Owed: re-recording as a Bazel target").
- **Proof:** `bazel test //warehouse:update_native_metadata_test`. `git grep -n "FormatData_en_US" warehouse/src/main/resources` matches only generated output.
- **Done when:** no section of the metadata file is hand-recorded.

#### A24 · Amend P1-16's proof

- **Change:** replace the `ls ${TMPDIR}` check with `git grep -n "legend-warehouse-duckdb\|java.io.tmpdir" warehouse/src/main` (empty). Add a `DuckLibraryTest` asserting `load(null)` fails with a message naming `--duckdb-library` rather than extracting.
- **Proof:** `bazel test //warehouse:tests`.
- **Done when:** reintroducing extraction fails a test.

#### A25 · Amend P1-19's proof

- **Change:** add a test-only `platform(name = "unlisted_test", constraint_values = ["@platforms//os:linux", "@platforms//cpu:ppc"])` in `//tools/platforms`.
- **Proof:** `bazel build --nobuild --platforms=//tools/platforms:unlisted_test //warehouse/... //pct/... //datacube/...` succeeds, with the native, DuckDB and Postgres targets reported as skipped incompatible, not as a select error.
- **Done when:** WH-N16's unconfirmed question is answered by a command in the item.

#### A26 · Amend P6-12 (G12) to agree with P3-29

- **Change:** G12 allows a `**` glob in a test's `data` only when the same set is passed as the test's argv (the scanner pattern), with a dated `.allow` row naming the scanner (`portability`, `guardrails`, `state-guardrail`).
- **Proof:** `bazel test //tools/guards:data_globs_test` passes after P3-29.
- **Done when:** G12 and P3-29 are both green.

#### A27 · Amend P3-16

- **Change:** `pivot-rows.ts`, `typed-values.ts` and `json-read.ts` await an app completion promise (expose `app.idle()`), and `quiet()`/`until()` are deleted. `query-store/test/conformance.ts:42, 67` use a counter for run IDs.
- **Proof:** `bazel test //datacube:tests //query-store:all`. `git grep -n "quiet()\|until(" datacube/test` is empty.
- **Done when:** no JS test polls with a time budget.

#### A28 · Amend P1-23 (low)

- **Change:** `node_test` asserts that exactly one entry point exists per target (the invariant that makes global DOM mutation safe), and says so in its docstring. A `//datacube:wasm_flag_test` checks that the `wasm = True` targets are exactly the test files importing `catalog-builder` or `lite-compiler`, read from `$(rlocationpaths)`.
- **Proof:** `bazel test //datacube:wasm_flag_test`. Negative: drop `wasm = True` from one WASM test, and the test fails.
- **Done when:** the WASM list cannot drift, and the one-process rule is enforced.

#### A29 · NEW P4-17 · `.mjs` files are typechecked

- **Change:** a `tsconfig.mjs.json` with `allowJs` and `checkJs` covers `datacube/demo/*.mjs`, `datacube/bench/*.mjs`, `tools/js/strict-reporter.mjs`, `fixtures/saved-queries/make.mjs`, `query/demo/*.mjs` and `site/*.mjs`. Add a `typecheck_mjs_test` beside `typecheck_test`. Land it after P4-01, so the new `harness.mjs` is typechecked from the start.
- **Proof:** `bazel test //datacube:typecheck_mjs_test`. Negative: a type error in a scratch `.mjs` fails.
- **Done when:** no first-party JS file escapes typechecking.

#### A30 · NEW P3-34 · DataCube tests depend on what they import (investigation first)

- **Change:**
  - **Question:** can `datacube/src` split into per-directory `ts_project`/`js_library` targets (for example `share`, `ui`, `layout`, `adhoc`) without import cycles?
  - If yes: each `node_test` depends only on the libraries it imports.
  - If no: a §6.3 row with the cycle evidence.
- **Proof:** touch one file in `datacube/src/share/`, and `bazel test //datacube:tests` re-runs only that file's dependents.
- **Done when:** the touch test re-runs a strict subset, or §6.3 records why not.

---

## 3. Appendix: finding → items

**F** = fully covered. **P** = partly covered; the row lists the items that cover the rest, and §2.1 gives the gap. Report N12–N14 of 04 are informational and ask for no action.

### Report 01

| Finding | Status | Items |
|---|---|---|
| CT-N1 fixed port | F | P0-07, P6-06 |
| CT-N2 CorpusDifferentialTest | P (#15) | P3-18 (action, test, SkipCensus row, javadoc, `Locale.ROOT`) |
| CT-N3 stress knobs, H2 lane | F | P3-12, P2-16 (`MIN_PASS_H2` as D9 policy) |
| CT-N4 locale | F | P0-10, P3-28, P3-18 (core main enforced; see #34/#35 for other modules) |
| CT-N5 order dependence | F | P3-03, P3-05 |
| CT-N6 child JVM | F | P3-19, P6-16 |
| CT-N7 render.sh | P (#10) | P3-13, P7-01 |
| CT-N8 host tmpdir | P (#25) | P0-10 |
| CT-N9 NoEagerTypeReferences | F | P3-27 (depends on A1's `java_jars` format change) |
| CT-N10 duckdb invisible | F | P3-02, P3-27 |
| CT-N11 timing and clock | F | P3-15 |
| CT-N12 dead and vacuous | P (#16) | P3-17, P6-13 |
| CT-N13 undeclared stress read | F | P3-02 |
| CT-N14 shared statics | P (#17) | P3-04 |
| CT-N15 row order | F | P3-11 |
| CT-N16 skipped roots | F | P3-14, P3-27 |
| CT-N17 hand pins | P (#18) | P2-16 (D9) |
| CT-N18 unread data | P (#19, #20) | P3-05, P2-13 |
| CT-N19 path-walking guards | P (#8) | P3-27, P3-32 |
| CT-K1 stress generators | F | P2-01, P2-02, P2-03, P6-01. The 14-versus-10 marker count is explained: four hand files cite one-shot codemods kept as history by the script review. |
| CT-K5 ladder writes the tree | F | P2-13, P6-07 |
| CT-K12 Repo resolver | P (#1–#3) | P1-03, P1-05, P3-27, P3-32 |
| CT-K15 coarse data, one java_test | P (#21, #23) | P3-05, P6-12 |
| CT-K18 scale lanes | P (#24) | P3-05 (the comment) |
| CT guardrails as JUnit | P (#9) | P3-27, P2-16, P7-14 |

### Report 02

| Finding | Status | Items |
|---|---|---|
| SP-N1 never-selected classes | P (#26, #27) | P3-17, P6-03 |
| SP-N2 hand-run tools | P (#11, #12, #28) | P3-13, P3-17, P7-08 |
| SP-N3 TokenDump, vocab.tsv | P (#29) | P2-18, P3-26 |
| SP-N4 engine-runner | F | P3-26, P3-25, P7-08, P7-15 |
| SP-N5 stale maven_runner lock | F | P0-05, P0-04, P1-25 |
| SP-N6 Postgres by host OS | F | P1-15 |
| SP-N7 EmbeddedPostgres | P (#30, #31) | P1-06, P3-20 |
| SP-N8 test protocol | F | P1-01, P1-02 |
| SP-N9 cumulative counters | P (#32) | P3-09, P3-08 |
| SP-N10 ChannelB order | F | P3-08 |
| SP-N11 MinimalCorpus wall clock | F | P3-15, P3-13, P6-04 |
| SP-N12 hand goldens and ledgers | F | P2-14, P2-15, P2-16 (constants stay hand-owned: D9) |
| SP-N13 in-test regeneration | F | P2-19 |
| SP-N14 hidden modes | F | P3-13, P2-17, P3-19, P6-04 |
| SP-N15 silent skips and empties | F | P3-14, P1-04, P6-13 |
| SP-N16 oracle manifests | P (#33) | P2-11 |
| SP-N17 classpath-order shims | F | P3-31 |
| SP-N18 temp files | F | P0-10, P3-20 |
| SP-N19 referee locale | P (#34) | P3-28, P0-10 |
| SP-N20 dead files | F | P7-15, P7-10, P7-12 |
| SP-N21 cross-module censuses | P (#7) | P3-30 |
| SP-N22 core-layers | F | P2-12; generating the golden is DEFERRED-OK by D9 |
| SP-N23 diagnostics as tests | F | P3-17 |
| SP-K5 writes into the tree | F | P2-17, P6-07 |
| SP-K6 prerun pipeline | F | P3-01 |
| SP-K7 cross-test output | F | P2-19 |
| SP-K10 Bump | P (#36–#38) | P0-05, P2-10; network in a release tool is DEFERRED-OK (P0-05 Risk) |
| SP-K12 runfiles | P (#4–#6) | P1-03, P1-04, P1-06, P3-32 |
| SP-K15 coarse inputs | P (#39) | P3-07 |
| SP-K16 shell genrules | F | P1-22 |
| SP-K17 pool disjointness | F | P6-11 |
| SP-K18 generators in test tree, duplicates | P (#40) | P7-12, P0-13, P3-09 |
| SP-K19 duplicate pins | F | P2-10 |

### Report 03

| Finding | Status | Items |
|---|---|---|
| WH-N1 reachability metadata | P (#41) | P2-09, P0-10 |
| WH-N2 DuckDB host temp cache (P0) | P (#42) | P1-16 |
| WH-N3 warehouse temp dirs | F | P0-10, P3-21 |
| WH-N4 packaging | F | P4-13 |
| WH-N5 Postgres driver | F | P0-08 |
| WH-N6 tests_native scope | F | P3-21 |
| WH-N7 user.name | F | P3-21 |
| WH-N8 locale | P (#35) | P0-10, P3-28 |
| WH-N9 engineScanOrder | F | P3-01 |
| WH-N10 product debug switches | P (#13, #14) | P7-14, P3-13 |
| WH-N11 JDBC service file | F | P4-14 |
| WH-N12 native access | F | P3-21 |
| WH-N13 sqlapi WASM | F | P3-24 |
| WH-N14 Maven references | P (#43) | P7-10, P7-06 |
| WH-N15 host C toolchain | F | P1-09, P1-10, P6-18; Windows MSVC is DEFERRED-OK (D5, P1-11) |
| WH-N16 selects, http | P (#44) | P0-12, P1-19 |
| WH-N17 cwd-relative runfiles | F | P1-05, P6-15 |
| WH-K8 gunzip, bash launcher | F | P1-17, P4-12 |
| WH-K14 pyarrow, host python | F | P1-07, P1-08 |
| WH-K18 live Postgres manual | F | P3-22 |
| WH-K11 jq platform filter | F | P5-03 |
| WH-K2 saved-query server | F | P2-06 |
| WH-K15 coarse data | P (#22) | P3-05 |
| WH-K19 lanes.sh | F (with A5) | P3-13 |

### Report 04

| Finding | Status | Items |
|---|---|---|
| DC-N1 hand-generated files | F | P2-08, P2-06, P4-15 |
| DC-N2 source scanners | P (#45) | P3-29, P3-10 |
| DC-N3 runfiles by hand | F | P1-24, P1-23, P6-15 |
| DC-N4 npm workflow | F | P4-15, P1-26, P0-01 |
| DC-N5 timing | P (#46) | P3-16 |
| DC-N6 clock, zone, locale | P (#47) | P3-10, P1-23, P3-29, P3-16 |
| DC-N7 bench | F | P4-15 |
| DC-N8 isolation | P (#48) | P3-10 |
| DC-N9 COPY TO | F | P4-16 |
| DC-N10 BUILD issues | P (#49, #50) | P4-15, P1-23, P0-13, P3-29 |
| DC-N11 pure-protocol, query-store | F | P1-23, P1-24, P3-16 |
| DC-K4 link-p2 redirect | F | P2-08 |
| DC-K9 make_dist | F | P4-11 |
| DC-K13 harnesses | F | P4-01–P4-09 |
| DC-K15 coarse data | P (#51) | (none for `:src`; portability globs via P3-29) |
| DC re-confirmed: node_options, chdir, live_snap, query-store lite | F | P1-23, P1-24, P3-29, P0-13, P3-16 |
| DC-N12, N13, N14 | informational | `warehouse.ts:208` is P3-16; DuckDB JNI extraction in `CatalogFacts`' action lands in P0-10's declared scratch directory |
