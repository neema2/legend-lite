# Area 3 report: the upstream-facing packages

Packages: `//parser-equivalence` (38 targets), `//pct` (32), `//scripts/parser` (10), `//tools/engine-runner` (10),
`//tools/bump` (4), `//tools/reference` (5), `//tools/par` (2), `//tools/wrongrows` (2). That is 103 targets, all
from `area3_targets.tsv`. Repo `runs/bazel-exec` at `6f1d9aa9a`. Read-only: I ran only `bazel query`, git and grep.

## Verdict counts

| verdict | n |
|---|---|
| WIRING | 39 |
| TOOL | 21 |
| TEST-CORPUS | 18 |
| GEN-COMMITTED | 8 |
| CHECK-DIFF | 6 |
| CHECK-GUARD | 6 (2 guards/scans, 4+1 manual reports) |
| GEN-BUILD | 2 |
| TEST-UNIT | 1 |
| TEST-INTEGRATION | 1 |
| DEAD | 1 |
| COMPILE | 0 (nothing in this area ships) |

## Evidence used throughout

- **E1, how a generator reruns.** `java_run` takes the **full transitive runtime jars** of its deps as action inputs
  (`tools/java_run/defs.bzl:46` `jars = depset(transitive = [d[JavaInfo].transitive_runtime_jars ...])`, `:110`
  `inputs = depset(ctx.files.srcs + ctx.files.roots, transitive = [jars, runtime.files])`). So any source edit in any
  library in the closure changes a jar and reruns the action. The ijar/early cutoff does not help here.
- **E2, the paths to core.** `bazel query 'somepath(<t>, //core:exec)'` (`//core:exec` is a core library that no
  generator here needs):
  - `gen_fixtures → harvest_lib → pe_tests_lib → //core:core → //core:exec`
  - `gen_manifest | gen_roster | gen_own_corpus_draft | ratchets | corpus_census → pe_tests_lib → //core:core → //core:exec`
  - `//pct:ratchets → pct_tests_lib → //core:core → //core:exec`
  - `//tools/engine-runner:vocab → runner → //core:core → //core:exec`
  - `//scripts/parser:keyword_coverage → keywords → //tools/engine-runner:vocab → runner → //core:core → //core:exec`
  - `//tools/engine-runner:smoke_test → lite_parse → runner → //core:core → //core:exec`
  - `//parser-equivalence:parser_parity → pe_tests_lib → //core:core → //core:exec`
  - empty for `//pct:adapter_par`, `//tools/reference:ref_dump` and `//tools/reference:ref_imports`. These do not reach core.
- **E3, `//core:core` is the umbrella.** It exports all 32 libraries (`core/BUILD.bazel:207-212, 233-236`). Every
  sub-library is private (`core/BUILD.bazel:19, 59`).
  `bazel query 'kind(java_library, deps(//core:parser)) intersect (//core:*+//base/...+//json/...)'` gives
  base, error, lexer, model, parser, protocol, spi, values, json. `deps(//core:diagnostics)` gives base and diagnostics.
- **E4, what parser-equivalence actually imports from core.** I grepped every `com.legend.<pkg>.<Class>` reference in
  `parser-equivalence/src/test/java`. Excluding the package's own classes, they are base, diagnostics, json, lexer,
  model, parser, protocol and testing, and nothing else. No compiler, planner, exec, sql, driver or server.
  `docs/EXECUTION_PLAN_2026_09_26.md:609` already says the same ("the PE tests import only parser, model, lexer,
  base, protocol, json and `//testing`") and plans the narrowing (W1.9).
- **E5, what runs today.**
  - `bazel build //...` builds every non-manual target. `.bazelrc` sets no tag filters (grep finds nothing).
    `bazel build //...` is CI lane `build` (`gates-run.yml:63`).
  - `//gates:local` (`gates/BUILD.bazel:15`) includes `//:generated`, which holds `//parser-equivalence:update_generated_tests`,
    `:update_ratchets_tests`, `//scripts/parser:update_keyword_coverage_tests`, `//tools/engine-runner:update_vocab_tests`
    and `//docs:update_generated_tests` (`BUILD.bazel:25-43`). `//:generated` is also CI lane `checks` (`gates-run.yml:52`).
    Query confirms it: `somepath(//gates:local, //parser-equivalence:gen_fixtures)` =
    `local → //:generated → update_generated_tests → update_generated_1_test → gen_fixtures`. The same holds for
    `gen_roster` (via `//docs:update_generated_test`) and `vocab`.
  - `//tools/guards:classpath_test` (in local) reaches every java_test, java_binary and java_run, manual ones included,
    through `guard_classpaths` (`tools/guards/defs.bzl:102-108`). That report is written from analysis providers only,
    with no action inputs (`tools/guards/classpath.bzl:27-39`). So it forces analysis of manual targets, not execution.
- **E6, the bump drives `//:update_generated`.** That covers every package's writer (`BUILD.bazel:51-71`). See Part A,
  `//tools/bump:bump`.

## Part A: every target

Shorthand: **B** = built by `bazel build //...` (CI lane `build`). **L** = in `//gates:local`'s execution closure.
**ck** = CI lane `checks` (via `//:generated` or by name). Lane numbers are from `gates-run.yml:51-66`. **man** = tagged manual.

### //parser-equivalence

| target | kind | what it is | reads that matters | produces | who uses it | SHOULD run on | runs TODAY | verdict | note |
|---|---|---|---|---|---|---|---|---|---|
| `all_files` | filegroup | `guards_package()` glob (`tools/guards/defs.bzl:109-116`) | package files | – | `//tools/guards:repository_files` | n/a | B | WIRING | used by guards |
| `corpus_census`, `grammar_keyword_census`, `migration_sizing`, `pmcd_reachability_census` | _java_run ×4, one comprehension (`BUILD.bazel:418-448`, same deps, flags, `tags=["manual"]`) | measurement report actions (P3-17). CorpusCensus: both parsers over the whole corpus (`CorpusCensus.java:54,88` PureGrammarParser + `Surfaces.platform`). GrammarKeywordCensus: .g4 keywords vs sources both parsers accept (`:61-73`). MigrationSizing: lite's legacy vs protocol parsers (`:124-139`). PmcdReachabilityCensus: static walk of the engine protocol graph plus roster (`-Dpe.roster`, no lite parser) | UPSTREAM_TREES, `:gen_fixtures`, `:gen_roster`, `:engine_jars_exec`, own-corpus test_java ×4, pins, plus all of core through `pe_tests_lib` | `bazel-bin/parser-equivalence/<name>/*.txt` + run.log | `:diagnostics_reports` (manual, 0 users); humans per `docs/GATES.md:61` | first three: upstream bump, or a change to parser/lexer/protocol/model, or the corpus. pmcd: bump only | man. Run by hand only. No workflow builds them; `diagnostics.yml` runs only `:diagnostics` (`.github/workflows/diagnostics.yml:48-50`) | CHECK-GUARD (report) | pmcd needs no lite code. All four carry all of core (E2) |
| `diagnostics` | java_test (junit_test, `BUILD.bazel:197-212`) | GrammarCoverageCensusTest, the asserting census | own corpus, sibling corpus, ledgers, engine jars, upstream trees (`upstream=True`) | test result | `.github/workflows/diagnostics.yml:50` | bump, parser/lexer/protocol/model change, corpus change | man. `diagnostics.yml` on paths `MODULE.bazel`, `release.MODULE.bazel`, `parser-equivalence/**`, `core/.../{parser,lexer,protocol}/**` (`:8-23`) | TEST-CORPUS | the path filter omits `core/.../model/**` and the own-corpus trees (`core|spec|pct/src/test`), which the test reads (E4, `BUILD.bazel:129-138`). The header (`diagnostics.yml:1-5`) says it runs censuses and benchmark; it runs only this test |
| `diagnostics_reports` | filegroup (`:450-455`) | groups the 4 censuses | – | – | humans (`docs/GATES.md:61`, BUILD comment `:417`) | n/a | man | WIRING | still documented |
| `engine_jars` | java_jars (`:90-95`) | runfiles-path list of every jar `pe_tests_lib` runs with | `pe_tests_lib` closure | jar list | `parser_parity`, `diagnostics` | follows its deps | B, lane 8 | WIRING | includes core's jars (transitive) |
| `engine_jars_exec` | java_jars (`:97-102`) | the same list, as exec paths, for actions | same | jar list | `gen_roster`, 4 censuses, `engine_jar_files` | follows its deps | B, L (via gen_roster) | WIRING | RosterGenerator filters to `legend-engine` jars (`RosterGenerator.java:66-70`) but the list carries core's |
| `engine_jar_files` | filegroup (`:104-109`) | output_group `jars` of the above, so the jars are action inputs | – | – | `gen_roster`, censuses | – | B, L | WIRING | |
| `fixture_sweep`, `probe_wire_shapes`, `z_fixture_adjudication_probe` | java_binary ×3, one comprehension (`:476-489`, same data, flags, manual) | on-demand probes (`bazel run`, `FixtureSweep.java:9`, `ProbeWireShapes.java:14`, `ZFixtureAdjudicationProbe.java:15`) | own corpus, ledgers, pins; sweep and Z use PmcdParser and PureGrammarParser | stdout/--out | humans only; `guard_classpaths` (analysis) | built when a human runs them | man | TOOL | |
| `gen_fixtures` | _java_run (`:280-297`) | FixtureHarvestGenerator: runs the engine's grammar and compiler tests-jars under recording shims (tier 1), compiles the engine tree's extension tests (tier 2) (`FixtureHarvestGenerator.java:25-62`) | `:harvest_tests_jars` (2 upstream tests-jars), `@legend_engine_src` tree, pins. **No lite code**: FixtureHarvest imports only java.* (`harvest/FixtureHarvest.java:3-23`); FixtureRecorder uses only `Corpus.FIXTURE_HEADER_PREFIX` and `OraclePins` (`FixtureRecorder.java:39-40`) | `generated/engine-grammar-fixtures.jsonl` → committed `src/test/resources/engine-grammar-fixtures.jsonl` (`:353-356`) | `update_generated_1(_test)`, `gen_manifest`, `gen_roster`, 4 censuses | **upstream bump only** (or an edit to the harvest code) | B, L, ck; reruns on **every core edit** (E1, E2) and every edit to any parity test class (shims depend on `pe_tests_lib`, `:228-230`) | GEN-COMMITTED | true trigger is not today's trigger. Also GEN-BUILD for gen_manifest and gen_roster (`_NEW_FIXTURES`, `:301`) |
| `gen_manifest` | _java_run (`:304-318`) | ManifestGenerator: SHA-256 per corpus source (`ManifestGenerator.java:27-35`) | UPSTREAM_TREES, `:gen_fixtures` output, pins. Core use: only `com.legend.diagnostics.Diagnostics` (`Corpus.java:275`) | committed `src/test/resources/corpus-manifest.tsv` | `update_generated_0(_test)` | **upstream bump only** | B, L, ck; reruns on every core edit (E2) | GEN-COMMITTED | needs `//core:diagnostics` + `//testing`, not `//core` |
| `gen_own_corpus_draft` | _java_run (`:392-413`) | OwnCorpusLedgerDraft: our PmcdParser vs the oracle over our own snippets; writes a draft with reasons kept (`OwnCorpusLedgerDraft.java:29-66`) | own snippets via `//core:srcs`, `//pct:srcs`, `//spec:srcs` (all of `src/**`), committed ledger, upstream trees, pins, core (needs parser+protocol) | `generated/own-corpus-protocol-diffs.tsv` → `docs/own-corpus-protocol-diffs.tsv` by `//docs:draft_own_corpus_ledger` (`docs/BUILD.bazel:27-34`, `diff_test=False`) | humans: `bazel run //docs:draft_own_corpus_ledger` (`docs/GATES.md:62-63`) | on human demand, after a parser/protocol change or an own-corpus change | **B (not manual)**: runs in every `bazel build //...` and reruns on every core edit, for a draft nobody consumes in the build | GEN-COMMITTED (draft) | should be manual. Over-declares `//core:srcs` (InlineSnippets reads only `<module>/src/test/**`, `InlineSnippets.java:114`) |
| `gen_roster` | _java_run (`:322-345`) | RosterGenerator: every `@JsonSubTypes` tag in the engine protocol jars, COVERED if **the oracle** parses a source reaching it (`RosterGenerator.java:59-126`; no lite parser) | engine jars, UPSTREAM_TREES (corpus), `:gen_fixtures`, our test snippets of core/spec/pct (`:115-118`) declared as `//core:srcs` etc. (all of `src/**`) | `docs/protocol-roster.tsv` via `//docs:update_generated` (`docs/BUILD.bazel:11-19`); also the censuses' `-Dpe.roster` | `//docs:update_generated(_test)`, 4 censuses | **upstream bump**, or an edit to an inline Pure snippet in `core|spec|pct/src/test/**/*.java` | B, L, ck; reruns on every core edit (compile closure E2 **and** `//core:srcs` covers main sources) | GEN-COMMITTED | core need: `//testing` + diagnostics only |
| `harvest_lib` | java_library (`:247-258`) | the harvest's classpath: shims first, then tests-jars | – | – | `gen_fixtures` | follows deps | B, L | TOOL | pulls `pe_tests_lib` → all of core |
| `harvest_shims` | java_library (`:222-241`) | engine-namespace recording shims + FixtureRecorder (P3-31) | `pe_tests_lib` (for Corpus/OraclePins) | jar | `harvest_lib` | – | B, L | TOOL | the only reason the harvest depends on core |
| `harvest_tests_jars` | java_jars (`:261-270`) | the 2 upstream tests-jars, declared | @maven_upstream | list | `gen_fixtures` | bump | B, L | WIRING | |
| `harvest_tests_jar_files` | filegroup (`:272-277`) | their files as action inputs | – | – | `gen_fixtures` | – | B, L | WIRING | |
| `parity_sources` | file_list (`:115-125`) | declared list of the files parity tests read (P3-27b) | sibling corpus, test_java of PE, core, pct, spec | list | parser_parity, diagnostics, 3 probes | – | B, lane 8 | WIRING | the right narrow shape: `test_java` only, not `srcs` |
| `parse_speed_benchmark` | java_binary (`:459-470`) | timing benchmark, `bazel run` | corpus, PmcdParser vs PureGrammarParser | stdout | humans (`docs/GATES.md:62`) | on demand | man | TOOL | |
| `parser_parity` | java_test (`:178-195`) | gate 8: every class in `com.legend.equivalence` except the census (package-selected) | our parser (E4), oracle jars, upstream trees, own corpus, sibling corpus, 9 ledgers, fixtures, manifest | test result | lane 8 (`gates-run.yml:58`) | change to parser/lexer/protocol/model, own corpus or sibling corpus, ledgers, PE tests, bump | lane 8 only (not L, `gates/BUILD.bazel:4-5`); compiled by B; reruns on **any** core edit (E2) | TEST-CORPUS | narrowing to `//core:parser`+`:diagnostics` is exactly W1.9 (`EXECUTION_PLAN_2026_09_26.md:607-613`) |
| `pe_tests_lib` | java_library (`:44-80`) | every PE class: tests, generators, censuses, probes (minus shims) | `//core` (umbrella), `//testing`, upstream grammar/compiler/protocol jars | jar | 18 targets (every PE program and test) | change to PE sources, or to the core libs it imports (E4) | B, L | TOOL (test library) | one library for 5 trigger classes. Should depend on `//core:parser` + `//core:diagnostics` (E3, E4), which needs area 1 to open their visibility |
| `ratchets` | _java_run (`:363-380`) | PeRatchets: `mutation.deck` size + `own_corpus.matched` (lite PmcdParser vs oracle over own snippets) (`PeRatchets.java:30-41`) | PE `:srcs`, core/pct/spec `:srcs`, upstream trees, pins, core parser | committed `src/test/resources/com/legend/equivalence/ratchets.tsv` | `update_ratchets(_test)` | parser/lexer/protocol/model change, own-corpus or sibling-corpus change, bump | B, L, ck; reruns on every core edit | GEN-COMMITTED | over-declares `//core:srcs` (main sources) |
| `sibling_corpus` | filegroup (`:21-24`) | the sibling parser's fixtures | – | – | 11 targets | – | B | WIRING | |
| `srcs` | filegroup (`:27-31`) | PE `src/**` for core guard tests | – | – | `//core:census_sources`, `:ratchets` | – | B | WIRING | |
| `test_java` | filegroup (`:15-18`) | PE test sources (the own corpus) | – | – | 11 targets | – | B | WIRING | |
| `update_generated` | _write_source_file (`:349-358`) | writer for fixtures + manifest | – | writes checkout | `//:update_generated` (bump) | bump | B (builds the gens) | WIRING | |
| `update_generated_0` | _write_source_file | per-file writer, manifest | `gen_manifest` | – | `update_generated` | – | B | WIRING | macro expansion |
| `update_generated_0_test` | _diff_test | committed manifest == `gen_manifest` | `gen_manifest` | – | `update_generated_tests` | bump (input changes only then, after the fix) | L, ck | CHECK-DIFF | today it reruns its generator on every core edit |
| `update_generated_1` | _write_source_file | per-file writer, fixtures | `gen_fixtures` | – | `update_generated` | – | B | WIRING | |
| `update_generated_1_test` | _diff_test | committed fixtures == `gen_fixtures` | `gen_fixtures` | – | `update_generated_tests` | bump | L, ck | CHECK-DIFF | as above |
| `update_generated_tests` | test_suite | the two diff tests | – | – | `//:generated` | – | L, ck | WIRING | used |
| `update_ratchets` | _write_source_file (`:382-388`) | writer for ratchets.tsv | `:ratchets` | – | `//:update_generated` | – | B | WIRING | |
| `update_ratchets_test` | _diff_test | committed ratchets == `:ratchets` | `:ratchets` | – | `update_ratchets_tests` | parser/corpus change, bump | L, ck | CHECK-DIFF | |
| `update_ratchets_tests` | test_suite | – | – | – | `//:generated` | – | L, ck | WIRING | used |

### //pct

| target | kind | what it is | reads that matters | produces | who uses it | SHOULD run on | runs TODAY | verdict | note |
|---|---|---|---|---|---|---|---|---|---|
| `adapter_par` | _java_run (`pct/BUILD.bazel:36-53`) | compiles the Pure adapter to its PAR, 4096 MB (`:49`) | `pct/src/main/resources/**`, `//tools/par:par_generator` (+ upstream compiled-core and TDS jars) | `pure-core_legend_lite_pct.par` | `adapter_par_jar` | adapter source edit, or upstream bump | B; lanes 6, 7, 7p, **9** (via `pct_tests_lib`). Not rerun by core edits (E2: no path) | GEN-BUILD | consumer: `adapter_par_jar`. Output not byte-reproducible (workplan `BAZEL_FIRST_CLASS_WORKPLAN_2026_10_03.md:1396`), so a rerun invalidates every PCT test downstream |
| `adapter_par_jar` | java_library (`:64-69`) | the PAR as a classpath resource | `:adapter_par` | jar | `:adapter` | – | B | WIRING | |
| `adapter` | java_library (`:57-62`) | adapter Pure source + PAR on the classpath | – | jar | `pct_tests_lib` | – | B | TOOL (test runtime) | |
| `all_files` | filegroup | guards_package | – | – | `//tools/guards:repository_files` | – | B | WIRING | |
| `discipline_sources` | file_list (`:136-140`) | declared list of `pct/src/**` for the scan | `:srcs` | list | `pct_discipline` | – | B, L | WIRING | |
| `pct_discipline` | java_test (`:142-155`) | source scan: no comparison machinery in PCT Java (G-02) | `pct/src/**` only; deps `//testing` + junit | – | `//gates:local` (`gates/BUILD.bazel:35`), `pct_duckdb` (lane 6) | an edit under `pct/src` | L, lane 6, B | CHECK-GUARD | well shaped: split off so local skips the PAR (`:134`) |
| `pct_duckdb_{essential,grammar,relation,standard,unclassified}` | java_test ×5, one comprehension (`:123-131`, same deps `pct_tests_lib`+`//core:drivers`, 4096 MB) | gate 6: the engine's PCT function suites through lite on DuckDB, one JVM per suite | interpreted PCT runtime (upstream jars), adapter PAR, all of core (channel A imports core builtin, exec, server, test, values, platform, model: grep of `pct/src/test/java/.../{extension,*.java}`) | – | `pct_duckdb` → lane 6 | any engine change on the execution path (compiler, planner, SQL, exec, server), PCT sources or adapter, bump | lane 6; B compiles | TEST-CORPUS | needs all of core legitimately |
| `pct_duckdb` | test_suite (`:157-160`) | 5 suites + discipline | – | – | lane 6 (`gates-run.yml:47`) | – | lane 6 | WIRING | used |
| `pct_h2` | java_test (`:170-179`) | gate 7: relation suite on H2 2.4.240 | as above + `@maven_h2_modern` | – | lane 7 | as above | lane 7; B | TEST-CORPUS | |
| `pct_postgres_{essential,...,unclassified}` | java_test ×5 (`:202-211`, same shape, `target_compatible_with`) | gate 7P: 5 suites on embedded Postgres 16 | as above + `@embedded_postgres` | – | `pct_postgres` → lane 7p | as above | lane 7p; B where compatible | TEST-CORPUS | |
| `pct_postgres` | test_suite (`:213-216`) | – | – | – | lane 7p (`gates-run.yml:48`) | – | lane 7p | WIRING | used |
| `pct_channel_b_{essential,...,unclassified}` | java_test ×5 (`:227-246`, same shape, `upstream=True`, 768 MB) | gate 9: **our** compiler and platform run the PCT sources from the pinned trees; verdicts diffed with channel A ledgers and the engine manifests (`channelb/ChannelB.java` doc) | upstream **source trees** + the engine's DuckDB manifest + its `Test_LegendLite_*_PCT.java`; core compiler, exec, lowering, probe, test, parser, model, protocol; `//core:drivers`. **Imports no `org.finos` class** (grep of `channelb/*.java`) | – | `pct_channel_b` → lane 9 | engine change (compiler/exec), PCT channel-B code, bump | lane 9; B. Builds the 4 GB adapter PAR and the interpreted PCT runtime it never loads (`somepath(pct_channel_b_relation, adapter_par)` = `→ pct_tests_lib → adapter → adapter_par_jar → adapter_par`) | TEST-CORPUS | over-depends: see Part C |
| `pct_channel_b` | test_suite (`:248-251`) | – | – | – | lane 9 (`gates-run.yml:59`) | – | lane 9 | WIRING | used |
| `pct_tests_lib` | java_library (`:71-108`) | all PCT Java (channel A, channel B, ratchets) | `//core`, `//testing`, upstream PCT jars, `:adapter`, `//core:shadow_binding` | jar | 17 targets | – | B; lanes 6/7/7p/9 | TOOL (test library) | one library serves channel A (needs upstream runtime) and channel B (does not) |
| `ratchets` | _java_run (`:256-272`, man) | PctRatchets: each channel-B suite's discovery count, by **running** the 5 suites (`PctRatchets.java` main) | UPSTREAM_TREES, `pct_tests_lib`, drivers, core | committed `src/test/resources/.../channelb/ratchets.tsv` | `update_ratchets(_test)` | bump, channel-B code, engine change that moves discovery | man; only via `//:update_generated` (the bump) or by hand. Its staleness is held by gate 9's tests (`:253-255`) | GEN-COMMITTED | |
| `srcs` | filegroup (`:16-20`) | `pct/src/**` | – | – | `//core:census_sources`, discipline, PE `ratchets`/`gen_roster`/`gen_own_corpus_draft` | – | B | WIRING | PE should take `test_java`, not `srcs` |
| `test_java` | filegroup (`:24-28`) | PCT test sources (part of the own corpus) | – | – | 11 PE targets | – | B | WIRING | |
| `update_ratchets` | _write_source_file (`:274-282`, man) | writer | `:ratchets` | – | `//:update_generated` | bump | via bump | WIRING | |
| `update_ratchets_test` | _diff_test (man) | committed pct ratchets == `:ratchets` | `:ratchets` | – | `update_ratchets_tests` only | – | **never**: manual, and its only suite is manual with 0 users | CHECK-DIFF | an unrun check; gate 9 carries the comparison |
| `update_ratchets_tests` | test_suite (man) | macro-made suite | – | – | **0 users** (inventory.json); no reference in `.github/`, `gates/`, docs or BUILD files (`git grep -n "update_ratchets_test" -- .github gates docs '*.md' '*.bazel'` finds only the PE and spec suites at `BUILD.bazel:36,39`) | – | nothing | DEAD | a by-product of `write_source_files`; delete it with `diff_test = False` (Part C) |

### //scripts/parser

| target | kind | what it is | reads that matters | produces | who uses it | SHOULD run on | runs TODAY | verdict | note |
|---|---|---|---|---|---|---|---|---|---|
| `all_files` | filegroup | guards_package | – | – | guards | – | B | WIRING | |
| `engine_grammars` | copy_to_directory (`scripts/parser/BUILD.bazel:54-59`) | the pinned `.g4` grammars as one tree | `@legend_engine_src//:grammars` | dir | `keyword_coverage` | bump | B, L | WIRING | |
| `our_pure` | copy_to_directory (`:61-70`) | the `.pure` we own, as one tree | `:pure_sources`, `//core:test_pure`, `//scripts/corpus:pure_sources` | dir | `keyword_coverage` | – | B, L | WIRING | |
| `pure_sources` | filegroup (`:19-22`) | `scripts/parser/**/*.pure` | – | – | `our_pure`, `keywords` | – | B | WIRING | |
| `tiers` | py_library (`:11-16`) | tiers.py for keywords.py | – | – | `keywords` | – | B | TOOL | |
| `keywords` | py_binary (`:34-50`) | the keyword census; `bazel run` report mode passes `--vocab` (`:41-42`) | data: grammars, `//tools/engine-runner:vocab`, our pure | – | `keyword_coverage` (as tool); humans | – | B, L | TOOL | its `data` carries `vocab` → runner → **all of core** (E2) into the run_binary tool |
| `keyword_coverage` | run_binary (`:73-94`) | the census as a golden: engine grammars vs our .pure | `:engine_grammars`, `:our_pure`. **Not** vocab: args pass no `--vocab` (`:81-88`; keywords.py reads it only if given, `keywords.py:116`). BUILD says so: "the vocabulary is the report's, not this" (`:72`) | `generated/keyword-coverage.tsv` → committed `scripts/parser/keyword-coverage.tsv` | `update_keyword_coverage(_test)` | **bump** (grammars) or a change to our `.pure` files | B, L, ck. Through the tool's runfiles it depends on vocab, so every core edit reruns vocab (E1) and invalidates this tool | GEN-COMMITTED | no Java or core input is real |
| `update_keyword_coverage` | _write_source_file (`:96-102`) | writer | – | – | `//:update_generated` | – | B | WIRING | |
| `update_keyword_coverage_test` | _diff_test | golden == generator | – | – | suite | bump, .pure change | L, ck | CHECK-DIFF | |
| `update_keyword_coverage_tests` | test_suite | – | – | – | `//:generated` | – | L, ck | WIRING | used |

### //tools/engine-runner

| target | kind | what it is | reads that matters | produces | who uses it | SHOULD run on | runs TODAY | verdict | note |
|---|---|---|---|---|---|---|---|---|---|
| `all_files` | filegroup | guards_package | – | – | guards | – | B | WIRING | |
| `runner` | java_library (`tools/engine-runner/BUILD.bazel:9-45`) | real legend-engine (`@maven_runner`) beside lite; 5 sources | `@maven_runner` jars, `//core` | jar | parse, lite_parse, testable, vocab | – | B, L | TOOL | only `LiteParseMain.java:66` uses core (`com.legend.parser.PmcdParser`). ParseMain, TestableMain, TokenDump and Cwd import no `com.legend` |
| `testable` | java_binary (`:47-58`) | TestableMain: runs `.pure` testSuites through the engine's Testable framework (README) | runner | – | `//scripts/corpus:run`, `:engine_stress`, `:testable_launcher` (`scripts/corpus/BUILD.bazel:209-220`), `smoke_test`, humans (README:17) | runner or @maven_runner change | B, L | TOOL | used |
| `parse`, `lite_parse` | java_binary ×2 (same comprehension) | ParseMain: engine parse per file. LiteParseMain: the lite twin via PmcdParser | runner | – | `smoke_test`; humans (`README.md:18-19`, `scripts/parser/HANDOFF.md:107-108`) | parse: @maven_runner. lite_parse: `//core:parser` | B, L | TOOL | |
| `smoke_test` | java_test (`:82-104`) | runs the 3 binaries through their launchers on `person.pure` (P3-26) | 3 binaries, fixture | – | `//gates:local` (`gates/BUILD.bazel:65`), lane misc | runner change, @maven_runner, `//core:parser` | L, misc, B; reruns on any core edit (E2) | TEST-INTEGRATION | |
| `vocab` | _java_run (`:62-71`) | TokenDump: every literal token the runner's ANTLR lexers know | `@maven_runner` jars only. TokenDump uses no lite code | `generated/vocab.tsv` → committed `tools/engine-runner/vocab.tsv` | `update_vocab(_test)`, `//scripts/parser:keywords` (data) | **upstream bump only** | B, L, ck; reruns on every core edit (E2: `vocab → runner → //core:core`) | GEN-COMMITTED | true trigger is not today's trigger |
| `update_vocab` | _write_source_file (`:73-79`) | writer | – | – | `//:update_generated` | bump | B | WIRING | |
| `update_vocab_test` | _diff_test | golden == generator | – | – | suite | bump | L, ck | CHECK-DIFF | |
| `update_vocab_tests` | test_suite | – | – | – | `//:generated` | – | L, ck | WIRING | used |

### //tools/bump, //tools/par, //tools/reference, //tools/wrongrows

| target | kind | what it is | reads that matters | produces | who uses it | SHOULD run on | runs TODAY | verdict | note |
|---|---|---|---|---|---|---|---|---|---|
| `//tools/bump:all_files` | filegroup | guards_package | – | – | guards | – | B | WIRING | |
| `//tools/bump:bump_lib` | java_library (`tools/bump/BUILD.bazel:5-10`) | Bump.java; no deps | – | jar | bump, bump_test | – | B, L | TOOL | |
| `//tools/bump:bump` | java_binary (`:15-19`) | **the upstream bump** (`Bump.java:21-51`). Phase 0: the release must be on Central; pure version derived from the engine pom; tag commits read over HTTPS; engine-managed versions read (`:93-117`). Phase 1: rewrite release.MODULE.bazel's PINS block, then `REPIN=1 bazel run @maven_upstream//:pin` and `@maven_runner//:pin` (`:119-142`); `--pins` stops here. Phase 2: `bazel run //:update_generated` (`:148-151`). Phase 3: `bazel test //...` (`:153-157`). Then the human "judgement half" (`:159-167`) | network, release.MODULE.bazel | rewrites pins, lock files, every generated file | humans: `bazel run //tools/bump -- <release>` (`docs/GATES.md:69-70`) | a new upstream release | by hand; B compiles it | TOOL | it drives **all** of `//:update_generated` (15 writers, `BUILD.bazel:54-70`). Not driven: censuses, `gen_own_corpus_draft`, `ref_dump`/`ref_imports`, `adapter_par` (build outputs rebuilt on demand) |
| `//tools/bump:bump_test` | java_test (`:23-36`) | the PINS-block rewrite on the real release.MODULE.bazel | `//:release.MODULE.bazel`, bump_lib | – | `//gates:local` (`:27`), ck (`gates-run.yml:52`) | Bump.java or release.MODULE.bazel change | L, ck, B | TEST-UNIT | well shaped: no core |
| `//tools/par:all_files` | filegroup | guards_package | – | – | guards | – | B | WIRING | |
| `//tools/par:par_generator` | java_library (`tools/par/BUILD.bazel:8-21`) | ParGenerator, run by `//pct:adapter_par` | upstream compiled-core and TDS jars | jar | `//pct:adapter_par` | – | B | TOOL | visibility pct only; correct |
| `//tools/reference:all_files` | filegroup | guards_package | – | – | guards | – | B | WIRING | |
| `//tools/reference:ref_resolutions` | java_library (`tools/reference/BUILD.bazel:51-58`) | RefResolutions.java over 37 pinned jars | @maven_upstream | jar | `ref_dump` | – | B | TOOL | |
| `//tools/reference:ref_dump` | _java_run (`:64-84`, man, 8192 MB) | legend-pure's compiler over core_relational's closure; dumps what every call resolved to | pinned jars only (E2: no core) | `ref-resolutions.tsv` (cached, not committed, not reproducible `:60-63`) | `//spec:reference_lane_report` (`spec/BUILD.bazel:230-247`) → manual `//spec:reference_lane` | **upstream bump only** (or RefResolutions.java) | man; built when `//spec:reference_lane` runs | GEN-BUILD | consumer `//spec:reference_lane_report`. The right shape already |
| `//tools/reference:ref_imports_lib` | java_library (`:87-94`) | RefImports.java, split so its edits never rerun the dump | @maven_upstream | jar | `ref_imports` | – | B | TOOL | |
| `//tools/reference:ref_imports` | _java_run (`:98-109`, man) | report: each source's implicit import group | pinned jars | `ref-imports.tsv` | humans: `bazel build //tools/reference:ref_imports` (`tools/reference/README.md:94`) | bump | man | CHECK-GUARD (report) | right shape |
| `//tools/wrongrows:all_files` | filegroup | guards_package | – | – | guards | – | B | WIRING | |
| `//tools/wrongrows:compare` | py_binary (`tools/wrongrows/BUILD.bazel:6-9`) | multiset comparison of engine vs lite rows | two directories given on the command line | report | 0 build users; humans: `tools/wrongrows/README.md:42`, `docs/GATES.md:5739` | on demand | B (built, never run) | TOOL | not DEAD (documented for humans) |

## Part B: the problems, with evidence

1. **Four generators whose true trigger is an upstream bump rerun on every core edit, in the local gate.**
   - `gen_fixtures` needs no lite code (`FixtureHarvest.java:3-23`, `FixtureRecorder.java:39-40`).
   - `gen_manifest` needs only `com.legend.diagnostics` (`Corpus.java:275`).
   - `vocab` needs no lite code: only `LiteParseMain` in `runner` touches core.
   - `keyword_coverage` runs no Java at all. It reaches core only through its tool's `data = vocab`, which it does not
     read (`scripts/parser/BUILD.bazel:72,81-88`).

   All four reach `//core:exec` (E2). `java_run` hashes the full runtime jars (E1). All four sit under `//:generated`,
   which is in `//gates:local` and CI `checks` (E5). Their outputs only change on a bump, so their diff tests should be
   cached on every engine edit. Today every core edit reruns the fixture harvest, and with it the tier-2 javac pass
   inside the action (`FixtureHarvestGenerator.java:52-54`).
2. **The mixed-trigger generators depend on more than they read, twice over.**
   - `gen_roster`, `ratchets` and `gen_own_corpus_draft` take `//core:srcs`, `//pct:srcs` and `//spec:srcs` as inputs.
     Those are `glob(["src/**"])`, main sources included (`core/BUILD.bazel:25-28`). The own-corpus walker reads only
     `<module>/src/test/**` (`InlineSnippets.java:114`). So a core main-source edit reruns them as a changed *data*
     input, even after the compile dependency is narrowed. `parser_parity` already uses the narrow `:test_java` lists
     (`parser-equivalence/BUILD.bazel:115-125`).
   - All three compile against the whole umbrella, but use at most parser, lexer, model, protocol, json and diagnostics
     (E3, E4). `gen_roster` uses none of the parser: it parses with the oracle alone (`RosterGenerator.java:103-126`).
3. **`gen_own_corpus_draft` runs in every `bazel build //...`, and its only consumer is a human `bazel run`.** It is
   not manual (`parser-equivalence/BUILD.bazel:392`). Its consumer, `//docs:draft_own_corpus_ledger`, is a
   no-diff-test writer for a hand-finished ledger (`docs/BUILD.bazel:21-34`). Every engine edit makes the build lane
   rerun a full own-corpus parity pass for a file nothing reads.
4. **`pe_tests_lib` is one library for five trigger classes.**
   - the gate-8 tests;
   - the upstream-only harvest (via `harvest_shims` deps `:pe_tests_lib`, `:228-230`);
   - the upstream-only manifest and roster;
   - the parser-triggered ratchets and draft;
   - the probes.

   Editing any parity test reruns the fixture harvest, the manifest and the roster.
5. **Channel B (gate 9) builds channel A's runtime.** Channel B imports only `com.legend.*` and junit (grep of
   `pct/src/test/java/org/finos/legend/lite/pct/channelb/*.java`). Its tests still take `pct_tests_lib`, which pulls the
   interpreted PCT runtime jars and the 4 GB adapter PAR: `somepath(//pct:pct_channel_b_relation, //pct:adapter_par)`
   = `→ pct_tests_lib → adapter → adapter_par_jar → adapter_par`. Because the PAR is not byte-reproducible (workplan
   `:1396`), each rebuild also invalidates every channel-B result downstream.
6. **A dead suite and an unrun diff test.** `//pct:update_ratchets_tests` has 0 users and no reference anywhere (Part A).
   Its member `//pct:update_ratchets_test` never runs. The BUILD comment says gate 9 holds the file instead
   (`pct/BUILD.bazel:280`).
7. **`//:update_generated` mixes "an upstream release moved" with "our code moved".** The bump regenerates through it
   (`Bump.java:148-151`). The same target is also how a human re-blesses a ratchet after a parser change, and how
   non-upstream generators in other packages run (`BUILD.bazel:54-70`). Nothing in the graph says which generators are
   upstream-triggered, so nothing stops them from also depending on the engine (problem 1).
8. **Stale docs.**
   - `tools/engine-runner/README.md:20` tells humans `bazel run //tools/engine-runner:token_dump`, but no such target
     exists (the program is `:vocab`, a java_run; `RunnerSmokeTest.java:18` says so).
   - `diagnostics.yml:1-5` claims to run censuses, sizers and the benchmark. It runs only `:diagnostics`.
   - Its path filter (`:8-23`) omits `core/src/main/java/com/legend/model/**` and the own-corpus test trees, which the
     test reads.
9. **Guards force analysis of manual targets.** `guard_classpaths` lists every `_java_run`, java_binary and java_test,
   manual ones included (`tools/guards/defs.bzl:102-108`). So `//tools/guards:classpath_test` in `//gates:local`
   analyzes the censuses, probes, `ref_dump`, `pct:ratchets` and the `diagnostics` test. This is analysis only
   (`classpath.bzl:27-39` writes from providers, with no inputs). It is an analysis cost, not an execution cost. Area 6
   (guards) owns it.

## Part C: the right shape for this area

Goals:
- (1) compiling stays compiling;
- (2) a generator runs only when its true trigger changes;
- (3) tests run where their kind says;
- (4) nothing depends on more than it needs.

Each proposal cites the evidence above.

**C1. Split parser-equivalence by trigger** (problems 1, 2 and 4; evidence E3, E4).
- `:corpus_lib`: Corpus, OraclePins, InlineSnippets, ModuleFiles. Deps: `//core:diagnostics`, `//testing`.
- `:harvest_shims` and `:harvest_lib` depend on `:corpus_lib`, not `:pe_tests_lib`. Then `gen_fixtures` depends on
  upstream jars plus the corpus library only, so it reruns only on a bump.
- `gen_manifest` depends on `:corpus_lib` only.
- `:roster_lib`: RosterGenerator. Deps: `:corpus_lib` plus upstream protocol jars, with no lite parser
  (`RosterGenerator.java:103`). `gen_roster` uses it. `:engine_jars_exec` lists only the upstream jars it needs, not
  `pe_tests_lib`'s whole closure.
- `:pe_tests_lib` depends on `//core:parser` + `//core:diagnostics` (+ `:lexer`, `:model`, `:protocol`, `//base`,
  `//json` for strict deps) instead of `//core`. This is plan item W1.9 (`EXECUTION_PLAN_2026_09_26.md:607-613`).
  **Depends on area 1** opening those libraries' visibility to `//parser-equivalence` (`core/BUILD.bazel:19,59`).
- Replace `//core:srcs`, `//pct:srcs` and `//spec:srcs` with the `:test_java` filegroups in `gen_roster`, `ratchets` and
  `gen_own_corpus_draft`. That is what `parity_sources` already does (`BUILD.bazel:115-125`), and what InlineSnippets
  reads (`:114`).

Result: an engine edit outside the front end leaves all of `//parser-equivalence` cached. A parser edit reruns
`parser_parity`, `ratchets` and the censuses. Only a bump or an own-corpus edit reruns `gen_roster`. Only a bump reruns
`gen_fixtures` and `gen_manifest`.

**C2. Tag `gen_own_corpus_draft` manual** (problem 3). It belongs with the censuses: built on request, run via
`//docs:draft_own_corpus_ledger`. `//docs` is another area; it also stays manual there.

**C3. Split engine-runner** (problem 1).
- `:engine_runner`: ParseMain, TestableMain, TokenDump, Cwd. `@maven_runner` only.
- `:lite_parse_lib`: LiteParseMain. Deps `//core:parser`.
- `vocab`, `parse` and `testable` depend on `:engine_runner`. Only `lite_parse` and `smoke_test` see core.

Then `vocab` reruns only on a bump. `smoke_test` reruns on parser edits only once `//core:parser` is visible
(**area 1**).

**C4. Take vocab out of the census tool** (problem 1). `:keywords` (the `run_binary` tool) drops
`//tools/engine-runner:vocab` from `data`. A separate `py_binary(name = "keywords_report", ...)` with the `--vocab` args
serves `bazel run` (`scripts/parser/BUILD.bazel:34-50`). Then `keyword_coverage` depends only on the pinned grammars and
our `.pure` files, which is its true trigger.

**C5. Carve channel B out of PCT** (problem 5).
- `:channelb_lib`: `channelb/*.java`. Deps `//core`, `//testing`, junit. Channel B uses core compiler, exec, lowering,
  probe and test, so the umbrella or `plan_side` + exec is legitimate here.
- `pct_channel_b_*` and `:ratchets` take `:channelb_lib` + `//core:drivers`, with no `:adapter` and no `@maven_upstream`.
- `pct_tests_lib` keeps channel A.

Lane 9 then never builds the 4 GB PAR.

**C6. `//pct:update_ratchets` gets `diff_test = False`** (problem 6). Gate 9 compares live discovery with the committed
file, so the dead suite and the unrun diff test go away. Alternatively, put `//pct:update_ratchets_tests` in lane 9. Pick
one.

**C7. Name the upstream-triggered generators, and make the bump their home** (problems 1 and 7). Add a
`//:update_upstream` (a `write_source_files` with `additional_update_targets`) listing exactly the writers whose true
trigger is the pin:
- `//parser-equivalence:update_generated` (fixtures, manifest);
- `//docs:update_generated` (roster: pin + own corpus);
- `//tools/engine-runner:update_vocab`;
- `//scripts/parser:update_keyword_coverage` (pin + our `.pure`);
- and the other areas' upstream writers (spec's natives and prelude, per `Bump.java:163`; areas 1/2 decide).

`Bump.java:148-151` runs `//:update_upstream` first. Then it runs the trigger-mixed ratchets (`//parser-equivalence:update_ratchets`,
`//pct:update_ratchets`), because a bump moves them too.

A guard (area 6) checks that every target under `//:update_upstream` has no path to `//core:core`'s execution
libraries. This is `bazel query 'somepath(<writer>, //core:exec)'` = empty, the query in E2, turned into a test. The
property then holds by construction rather than by review. The diff tests stay in `//:generated` (local and `checks`).
Once C1, C3 and C4 land they are cache hits on every engine edit, so they cost nothing there and still catch a hand
edit of a generated file.

**C8. Keep as is.** These already have the right shape:
- `//tools/reference` (`ref_dump`/`ref_imports`: manual, upstream jars only, `RefImports` split so its edits never rerun
  the dump);
- `//pct:adapter_par` (no core; but see the reproducibility item, workplan `:1396`);
- `//pct:pct_discipline`;
- `//tools/bump:bump_test`;
- `//tools/par`;
- `//tools/wrongrows:compare`.

The PCT channel-A suites (gates 6, 7, 7P) legitimately depend on all of core. Their trigger really is any engine change,
and they belong in CI lanes, not local, as today.

**C9. Docs** (problem 8). Fix `tools/engine-runner/README.md:20` (`:token_dump` becomes `:vocab`/`:update_vocab`).
Correct `diagnostics.yml`'s header and add `core/src/main/java/com/legend/model/**` and `{core,spec,pct}/src/test/**`
to its path filter. After C1 the filter can simply follow what `pe_tests_lib` depends on.

**What I depend on others deciding:**
- area 1: visibility of `//core:parser`, `:diagnostics`, `:lexer`, `:model`, `:protocol` to `//parser-equivalence` and
  `//tools/engine-runner`;
- the area owning `//:generated`, `//:update_generated` and `//docs`: C2 and C7;
- area 6 (guards): the C7 guard, and whether `guard_classpaths` should skip manual targets (problem 9).

**Not settled:**
- Whether `PctRatchets.runSuite` needs the `pct.suite.*`/`pct.oracle.*` properties that only the tests pass.
  `pct/BUILD.bazel:256-272` does not pass them. Settling it means reading `ChannelB*Test.runSuite` or running
  `bazel build //pct:ratchets`, which this brief does not allow.
- C1 needs one compile with the narrowed deps to prove strict deps are satisfied. E4 is a grep, not a compile.
