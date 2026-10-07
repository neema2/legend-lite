# Bazel audit, slice 6: scripts, tools, docs and CI

Audited at origin/main 16c8120d5 (2026-10-02)

**Slice:** every file under `scripts/` (for `scripts/corpus`: `build.py` and every module it imports), every non-Java file under `tools/`, every script under `docs/`, `experiments/`, `repro/`, `projects/`, `nlq/`, the three `.github/workflows` files, and the human-facing build and test docs (README.md, AGENTS.md, FAQ.md, docs/GATES.md, docs/*BAZEL*, every package README).

I modified nothing; `git status` was clean afterwards. My only executions were in-memory Python reads with bytecode writing turned off (`PYTHONDONTWRITEBYTECODE=1 python3 -B`). They compared committed stress files with their generators and wrote nothing.

**Repo-wide facts used below**
- 135 tracked `.py` files. **Zero** Bazel rules use them: no `py_binary`, `py_test` or `rules_python` anywhere in MODULE.bazel or any BUILD/.bzl.
- Tracked `.sh` files: 4 in tools, 3 in docs, 4 in experiments.
- Tracked `.mjs` files in my slice: 4 under docs, 3 under `datacube/bench`.
- `nlq/` is **not tracked** and does not exist, yet FAQ.md describes it as a module (FAQ.md:346 and the NLQ section around 255–303).
- `docs/STANDARD_BUILD_PROGRAM.md` **does not exist**. No tracked file matches `STANDARD_BUILD*`.

---

## Section 1 — Script inventory

**Classes:**
- **A** — writes or produces something committed or consumed. It must become a Bazel action plus a `write_source_files` diff test.
- **B** — a useful developer or analysis tool. It should become a `py_binary`, `sh_binary` or `java_binary` target.
- **C** — dead or one-shot. Delete it or move it to an attic.

"cp.txt" means the Maven-era `tools/engine-runner/cp.txt` classpath. Under Bazel it no longer exists: `tools/engine-runner/.gitignore` still lists `target/` and `cp.txt`, and its README never mentions it. Every script that reads it is broken.

### 1a. `scripts/corpus` — the generator `build.py` and its 26-module import closure (stdlib only)

Closure (computed from every `import` line): aggregate, aggregates, battery, combos, deepstack, density, emit, executed, expand, flat, functest, graphs, hier, model, oracle, partition, quarantine, query, rhs, seed, spread, stacking, stacks, taxonomy, tomany, views.

| path | lines | last commit | writes | consumers | class | fix |
|---|---|---|---|---|---|---|
| scripts/corpus/build.py | 398 | 2026-08-22 | Writes exactly 7 files in `core/src/test/resources/stress/`: 92-services, 93-testdata, 94-fanout-services, 95-function-tests, 96-external-data, 97-hier-execution, 98-combination-execution (build.py:42-48, 360-388). `--check` compares in memory. | `//core:stress_suites` via `_STRESS_READS` (core/BUILD.bazel:268, 397). | A (KNOWN K1) | Make it a `py_binary` plus a genrule feeding `write_source_files` in `//core:update_generated`. Prerequisites are the three blockers below. |
| model.py | 2076 | 2026-08-24 | none | build closure | A (part of K1) | `STRESS` and `PROJECTS` are paths resolved via `__file__` (model.py:32-33). These must become declared inputs. |
| query.py | 437 | 2026-08-22 | none | build closure | A | **Reads `92-services.pure`, which is also an output** (query.py:399). See blocker 1. |
| oracle.py | 2749 | 2026-08-24 | none | build closure | A | Uses `zoneinfo` (oracle.py:1563-1569); see blocker 2. The `today`/`now` evaluators (1842-1848) are unused by the current outputs. |
| seed.py | 21306 | 2026-08-24 | none | build closure | A | data module |
| battery, emit, flat, functest, graphs, hier, aggregates, aggregate, combos, deepstack, expand, partition, quarantine, rhs, spread, stacks, taxonomy, tomany, views | 46–2546 each | 2026-08-13 … 2026-08-24 | none (library modules) | build closure | A | Same target as build.py. |
| density.py | 174 | 2026-08-15 | none; `--gate` exits 1 on ratchet breach (density.py:145-157; constants 168-169) | Imported by build. The `--gate` ratchet is run by nothing. | A/B | Add a `py_test` running `--gate`, or fold it into the build check. |
| executed.py | 459 | 2026-08-19 | none; `--gate` ratchet (456); `BASELINE` (426) | build calls `executed.regressions` | A | Covered once build.py is gated. |
| stacking.py | 262 | 2026-08-19 | none; `--gate`/`--gaps` with `SURFACE_BASELINE` (44, 233-252) | spread imports it | A/B | Add a `py_test` for `--gate`. |

**Three blockers to making build.py a hermetic Bazel action (all NEW):**

1. **Input equals output.** `92-services.pure` holds the hand-written queries *and* the generated expectations. `query.load()` parses the committed file (query.py:399) and build.py rewrites it in place (build.py:376-381). A Bazel action cannot read its own output.
   - The hand-authored query source must be split into a separate committed file.
   - `model.load()` also globs *all* of `STRESS/*.pure`, generated files included (model.py:1889, 1950, 1981, 2021). So does `_id_collisions` (build.py:283).
2. **Host time-zone data.** `zoneinfo` reads the host's tz database, and Windows has none. 17 time-zone uses appear in `94-fanout-services.pure` and 1 in `92-services.pure`. A hermetic action needs the `tzdata` pip package pinned.
3. **Three more "GENERATED" stress files outside build.py, with no writer at all:**
   - `59-dense-mapping.pure` claims `dense_mapping.py`, `60-dense-store.pure` claims `dense_store.py`, and `64-combinations.pure` claims `combos.py`. The `__main__` blocks of those scripts only print statistics. `combos.build_source()` (combos.py:435) has no caller.
   - In-memory comparison:
     - **59 DIFFERS** from `dense_mapping.build()` (1447 committed lines vs 1357 generated).
     - **60 DIFFERS** from `dense_store.build()` (83 vs 78).
     - **64 MATCHES** `combos.build_source()`.
   - So the "do not edit by hand" headers are false for 59 and 60, and build.py `--check` does not cover any of the three.

### 1b. `scripts/corpus` — modules outside the build closure

| path | lines | last commit | writes | consumers | class | fix |
|---|---|---|---|---|---|---|
| add_taxonomy_edges.py | 144 | 2026-08-22 | Edits committed stress `8*.pure` and `30-store.pure` in place (lines 120, 136) | none | C (one-shot codemod) | attic |
| brokerage.py | 231 | 2026-08-19 | Edits `30-store.pure` and `89-all-mapping.pure`; writes `836-brokerage.pure` (169-222). `ROOT=…/exec` points at a directory that does not exist. | none | C | attic |
| curves.py | 378 | 2026-08-19 | Edits 30-store and 89; writes `835-curves.pure` (322-373). Uses the missing `exec/` directory. | curves2 imports it | C | attic |
| curves2.py | 241 | 2026-08-19 | Edits 30-store and 835 (139-199) | none | C | attic |
| largeexp.py | 402 | 2026-08-20 | Edits 30-store and 89; writes `8110-largeexp.pure` (296-397) | none | C | attic |
| schedule.py | 169 | 2026-08-19 | Edits 30-store and 89; writes `837-schedule.pure` (101-164) | none | C | attic |
| timeseries.py | 354 | 2026-08-19 | Edits 30-store and 89; writes `834-timeseries.pure` (302-349). Uses the missing `exec/` directory. | none | C | attic |
| refdata.py | 165 | 2026-08-20 | Edits 30-store and 89; writes `8<idx>-<pkg>.pure` (46-147) | taxa_* import it | C | attic |
| taxa_exotics / taxa_infra / taxa_markets / taxa_more / taxa_ops | 319 / 480 / 252 / 251 / 253 | 2026-08-20 | Through refdata, edit the committed stress sources | none | C | attic |
| dense_mapping.py | 143 | 2026-08-14 | Prints only. `build()` would produce 59, which has since diverged. | none | A (orphaned generator) | Either wire it to 59 through `write_source_files` or drop the GENERATED header and delete the script. |
| dense_store.py | 206 | 2026-08-14 | Prints only. Would produce 60, which has diverged. | none | A (orphaned) | Same as dense_mapping.py. |
| coverage.py | 150 | 2026-08-13 | `docs/corpus-coverage.json` with `--snapshot` (89, 118). Reads `parser-equivalence/target/protocol-roster.txt` and cp.txt (47-55). | none in Bazel | C | Delete it and the json. Bazel already generates the roster (`//docs:update_generated`). |
| differential.py | 158 | 2026-08-13 | `core/target/diff/{seed.sql,expected/*}` (36, 114-144) | `CorpusDifferentialTest`, which reads `Repo.out("diff")`. That resolves to `TEST_UNDECLARED_OUTPUTS_DIR/diff` (testing/.../Repo.java:129-133), which this script can never fill. **The test is skipped in every build.** | A | Turn it into a genrule whose output is the test's `data`; otherwise delete both. |
| functions.py | 358 | 2026-08-16 | none; reads `docs/ENGINE_FUNCTIONS.tsv` and `docs/FUNCTIONS_EXECUTED.tsv` (65, 128) | none | C | attic |
| mutate.py | 359 | 2026-08-13 | Temporary copies; runs the engine through cp.txt (270-320) | none | C (broken) | Delete, or rebuild on `//tools/engine-runner:testable`. |
| run.py | 208 | 2026-08-22 | none; runs the engine through cp.txt and `JAVA_HOME=~/jdk/jdk-21.0.11+10` (29-31, 90-92) | imported by 16 probe scripts | C (broken) | Replace with a Bazel test over `//tools/engine-runner`. Today only `tools/wrongrows/engine-rows.sh` does this job. |
| scoreboard.py | 232 | 2026-08-16 | none; reads `docs/ENGINE_SURFACE.tsv` and `docs/SURFACE_BLOCKED.tsv` (36, 204) | none | C/B | attic |
| probe_aggregates, probe_boundary_navigation, probe_collection, probe_column_types, probe_derived_filter, probe_extends_filter, probe_functions, probe_graphfetch_included_mapping, probe_ineq_aggregate, probe_milestoned_join, probe_missing_setid, probe_project_deps, probe_qualified_broken_chain, probe_relation, probe_remaining, probe_tds (16 files) | 132–595 | 2026-08-16 … 2026-08-22 | Temporary `.pure` files. The record path rewrites committed `docs/FUNCTIONS_EXECUTED.tsv` (probe_functions.py:480-516). All need cp.txt. | Reproduction instructions in `repro/*/README.md` | C (broken) | Move to an attic with `repro/`, or re-express as `bazel run //tools/engine-runner:testable -- <files>`. |
| `scripts/corpus/repro/**` (31 `.pure` + README) | — | 2026-08-12 … 2026-08-16 | n/a | README.md:6-72 says `cd tools/engine-runner && mvn -o compile` and `java -cp … $(cat cp.txt)` | C (documents) | Rewrite as `bazel run` commands, or move to an attic. |
| `scripts/corpus/verified/**` (4 `.md` + 32 `.pure`) | — | 2026-08-14 | n/a | Cited only as provenance in stress comments (`60-dense-store.pure:7`, `62-mapping-features.pure:10`) | C (keep as docs) | none |

### 1c. Other scripts

| path | lines | last commit | writes | consumers | class | fix |
|---|---|---|---|---|---|---|
| scripts/census_gate.py | 216 | 2026-08-12 | `docs/census-baseline.json` (151); `parser-equivalence/target/*` (34, 65). Runs `mvn` from `~/jdk/apache-maven-3.9.9` (38-42). | none | C (KNOWN K19) | Delete it and `docs/census-baseline.json`. |
| scripts/generate_pure_constants.py | 337 | 2026-04-25 | `engine/src/main/java/com/gs/legend/compiler/Pure.java` (28-29) — the engine module is deleted | none | C (KNOWN K19) | delete |
| scripts/outstanding.py | 163 | 2026-07-22 | `docs/OUTSTANDING.md` (130). Hard-coded `/Users/<user>/legend/legend-lite` and a legend-engine checkout path (14-18). | none (AGENTS.md:352 calls the output history) | C | delete |
| scripts/walldepth.py | 28 | 2026-07-22 | `docs/WALL_DEPTH.txt` (22), from the git history of the frozen `RELATIONAL_CORPUS.md` | none | C | delete |
| scripts/parser/fixtures.py | 315 | 2026-08-14 | none; runs `perf.ParseMain` through cp.txt, `target/classes` and `~/jdk` (36, 61-68) | none | C/B | See the fixture-corpus note after this table. |
| scripts/parser/keywords.py | 397 | 2026-08-14 | none; reads `tools/engine-runner/vocab.tsv` (103) | none | B/C | Same note. |
| scripts/parser/mutants.py | 289 | 2026-08-14 | Committed `scripts/parser/mutants.tsv` (39, 269); needs cp.txt (204) and `/tmp` | none. Superseded by `parser-equivalence/.../MutationFuzzTest.java`, which generates its own mutants. | C | Delete it and `mutants.tsv`. |
| scripts/parser/parity.py | 163 | 2026-08-14 | none; needs cp.txt and `core/target/classes` (36-52) | none | C | Fold into parser-equivalence. |
| scripts/parser/tiers.py | 275 | 2026-08-13 | library | keywords.py | C/B | as above |
| scripts/projects/check.py | 174 | 2026-08-22 | none; needs cp.txt and `perf.TestableMain` (118-123) | `projects/CONTRACT.md:3, 72` and every `projects/*/MANIFEST.md` | **A-equivalent (NEW)** | **The `projects/` contract — every project compiles alone, and the graph compiles together — is checked by nothing under Bazel.** `//projects:srcs` feeds only core's stress reads (core/BUILD.bazel:268, 273). 45 of the 56 projects are never compiled. Fix: a Bazel test per project (the planned `legend_library`, projects/BUILD.bazel:3). |
| scripts/projects/loadtime.py | 122 | 2026-08-20 | none; cp.txt | none | B | A `java_binary` benchmark, or delete. |
| scripts/projects/spec.py | 256 | 2026-08-20 | none (the project matrix) | check.py | C/B | as above |
| tools/census/lanes.sh | 20 | 2026-10-02 | `runs/census/<label>/*` (gitignored), copied out of `bazel-testlogs` | README | B (NEW) | Should be an `sh_binary` or test runner. It wraps `bazel test` in shell. |
| tools/census/render.sh | 15 | 2026-10-02 | `runs/census/render/*`. **Hard-codes** `$(bazel info output_base)/external/rules_java++toolchains+remotejdk25_macos_aarch64/bin`, so it works only on macOS arm64 (render.sh:9). Compiles `tools/census/RenderCensus.java` with a bare `javac`; `tools/census` has no BUILD file. | README | B (NEW) | Add a `java_binary` for RenderCensus that depends on `//core:core_tests_deploy.jar`. |
| tools/census/lanes_diff.py | 45 | 2026-10-02 | stdout | README:12 | B | `py_binary` |
| tools/ci-watch.sh | 18 | 2026-09-13 | stdout; `curl` to the GitHub API plus host `python3`; repo name hard-coded | Four dated docs | C/B (KNOWN K19) | Delete; `gh run watch` does the same. |
| tools/golden_shape_survey.py | 240 | 2026-08-29 | Committed `docs/golden-shape-survey-4AD.tsv` (16, 210). Default engine root `/Users/<another user>/legend/legend-engine` (14) — a different user's home. | `docs/NAV_ROUTING_BATCH0_4AD.md` | C (KNOWN K19) | attic |
| tools/metamodel-census/{build,closure,props,scan2,scan3}.py | 97 / 60 / 56 / 48 / 64 | 2026-09-02 | Committed `tools/metamodel-census/*.json` (8 files: closure, fallback_partition (0 bytes), family_tests, hn_vocabulary_tests, inventory, inventory_props, scan3, shapes). Hard-codes `/Users/<another user>/legend/*` and `core/target/wholetest-flipped.txt`. | `docs/METAMODEL_AS_RELATIONS_HOMEWORK_2026_09_02.md` and `docs/SESSION_HANDOFF_2026_09_02.md` | C (KNOWN K19) | Attic, with the JSON receipts. |
| tools/native-axes.py | 160 | 2026-09-10 | Optional `--tsv`; reads `core/target/lowering-coverage-probe.txt` (12, 99). Cites the missing **`tools/oracle-roots.sh`** (20). | UPSTREAM_BOUNDARY_PROGRAM.md:13 | B/C | Rebuild on the `@legend_*_src` repos, or delete. |
| tools/upstream-drift.py | 274 | 2026-09-11 | stdout; shells out to `curl` (54) and `git ls-tree` on local checkouts (35-36, 79). Cites the missing `tools/oracle-roots.sh` (15) and `tools/version-report.sh` (52). | Javadoc in `UpstreamPathManifestTest.java:29`; UPSTREAM_BOUNDARY_PROGRAM.md | B | A `py_binary` reading the `@legend_*_src` repos; drop the host-checkout dependency. |
| tools/scoreboard.py | 130 | 2026-07-08 | Appends to `docs/SCOREBOARD.md` (114-116). Needs `engine/target/surefire-reports` and `mvn` (4-15). | none | C | delete |
| tools/spikes/fusion_{probes,spike,spike2}_2026_08_28.py | 50 / 272 / 220 | 2026-08-28 | stdout; needs host `duckdb` Python | `docs/V12_FUSION_SPIKE_2026_08_28.md` | C (KNOWN K19) | attic |
| tools/untangle/move_classes.py | 231 | 2026-09-26 | Rewrites sources, `pom.xml`, BUILD files; uses `git mv` (35-56) | `docs/plan-audit-2026-09-26/cycles.md`, GATES.md 6667+ | B/C (KNOWN K19) | Codemod: attic once the untangle is done. `pom.xml` handling is dead because no poms remain outside experiments. |
| tools/untangle/bare_tiers.py, probe_counts.py | 71 / 123 | 2026-09-27 | `--out` TSV from `shadow.tsv` in the `test.outputs` directories | GATES.md, EXECUTION_PLAN:42 | B | `py_binary` |
| tools/untangle/groups.txt | 32 | 2026-09-27 | data | move_classes.py | C | goes with move_classes.py |
| tools/reference/join.py | 197 | 2026-09-26 | stdout | Superseded: the README says it was ported into Java (`ReferenceJoin`) for `//spec:reference_lane` (README:3-9) | C | delete |
| tools/reference/source_drift.py | 50 | 2026-09-26 | `out.tsv` | README calls it history (README:6) | C | delete |
| tools/reference/RefImports.java (Java; noted because the README documents running it by hand) | — | — | — | Not in `tools/reference/BUILD.bazel` | B | `java_binary`, or delete |
| tools/wrongrows/engine-rows.sh | 38 | 2026-09-30 | `<rows-dir>/*`. Copies jars out of runfiles and uses the hard-coded `remotejdk25_macos_aarch64` java (24), so macOS arm64 only. Carries a **third copy of LINKED_PROJECTS** (line 13; the others are model.py:59 and StressCorpus.java:26). | README, GATES.md 5691, 5721 | B (NEW) | Should be a Bazel test or `run_binary` over `//tools/engine-runner:testable`. It is the only engine-side stress run. |
| tools/wrongrows/compare.py, damage.py | 116 / 236 | 2026-09-30 | stdout / `--out` `.pure` | README:38, 48 | B | `py_binary` |
| tools/oracle-pins.env | 40 | 2026-09-23 | data. Duplicates MODULE.bazel's release, held equal by `//tools/deps:one_release` (tools/deps/BUILD.bazel:94-106). Read by `OraclePins.java` and 5 other parser-equivalence tests. Also a trigger path in diagnostics.yml:12, 20. | — | KNOWN K19 | Generate it from MODULE.bazel (a genrule) instead of guarding two hand copies. |
| tools/engine-runner/vocab.tsv | 91 | 2026-08-13 | Committed. Generated by hand with `java … perf.TokenDump > vocab.tsv` (scripts/parser/README.md:132-136, HANDOFF.md:84). | Only `scripts/parser/keywords.py` | A/C (NEW) | Delete with `scripts/parser`, or produce it from `//tools/engine-runner:token_dump` with a diff test. |
| tools/engine-runner/.gitignore | 2 | 2026-08-12 | `target/`, `cp.txt` | — | C (Maven leftover) | delete |

**`scripts/parser` fixture corpus note.** `scripts/parser/fixtures/` holds about 50 positive `.pure` files and `negative/` about 200 more, plus `parity-quarantine.tsv`. Nothing in Bazel consumes any of them: no BUILD file, `.bazel` file or Java source references them. The fix is to make them `data` of a parser-equivalence verdict test (the engine-vs-lite comparison parity.py did), or delete them.

### 1d. Scripts under `docs/` (24 = 17 `.py` + 3 `.sh` + 4 `.mjs`; plus 4 `.ts` and 15 `.java` probes)

All are dated audit receipts. **Class C** throughout: none is consumed by any build, test or CI step.

| path | lines | last commit | writes | notes |
|---|---|---|---|---|
| docs/burndown-2026-08-14/tools/census3.py | 48 | 2026-08-14 | `$BURNDOWN_OUT` or `/tmp/burndown/engine-tests.csv` | `/Users/<another user>/...` engine root |
| …/cluster.py | 45 | 2026-08-14 | `/tmp/burndown/clusters.json` | |
| …/dossier.py | 50 | 2026-08-14 | `/tmp/burndown` | `/Users/<another user>` |
| …/famdiff.py | 27 | 2026-08-14 | stdout | `/Users/<another user>/legend/legend-lite/docs/RELATIONAL_CORPUS.md` |
| …/features.py | 49 | 2026-08-14 | `/tmp/burndown/features.json` | |
| …/ledger.py | 19 | 2026-08-14 | `/tmp/burndown/failing.{json,tsv}` | |
| …/master.py | 39 | 2026-08-14 | `/tmp/burndown/master.csv` | |
| …/recon3.py | 31 | 2026-08-14 | stdout | |
| docs/invention-audit-2026-08-14/probes/{bare,cls,final,idx,nat,usage2,usage3}.py | 14 / 26 / 19 / 27 / 14 / 30 / 34 | 2026-08-14 | `$CLAUDE_JOB_DIR/tmp/audit/*.json` | `/Users/<another user>` paths |
| docs/parked/batch120_partA.py, batch120_partA2.py | 288 / 237 | 2026-09-07 | Rewrite Java sources in place | codemods |
| docs/type-audit-2026-08/harness/setup.sh, jrun.sh, probe.sh | 16 / 15 / 8 | 2026-08-26 | `.cp` | **`mvn -pl core …`** (setup.sh:9-10). Dead. |
| docs/datacube-dashboards-homework-2026-09-28/charts/render-check-{echarts,plot,vega-csp}.mjs | 10 / 3 / 5 | 2026-09-28 | stdout | Need an npm install of `survey-package.json` |
| …/gridstack/measure-gridstack.mjs | 39 | 2026-09-28 | stdout | |
| …/layout-prototype/{tile-layout,tile-layout.test,fuzz-deep,probe}.ts | 388 / 381 / 35 / 18 | 2026-09-28 | — | `node --experimental-strip-types --test` (test.ts:3). A prototype test never run. |
| Java probes: docs/invention-audit/probes (9), docs/parser-audit/probes (5), docs/type-audit/harness/Probe.java, docs/parked/InDbVerdict.java | — | — | — | Run by hand |

Fix for all of these: move to an attic, or keep as receipts but move them out of `docs/`.

### 1e. `experiments/`, `repro/`, `projects/`

**experiments/**
- It is in `.bazelignore`. The comment there says "Two Bazel prototype workspaces", but the directory holds much more: backend-probes (about 120 TSVs, 8 `.py`, `harness/pom.xml` plus `Probe.java`, `databricks/DbxTest.java`, `duckdb-census/*`), name-resolution-repro, postgres-dialect, warehouse-ffm, warehouse-w0, warehouse-w1d.
- Nothing outside it that matters references it: no BUILD, `.bzl`, Java, TypeScript or CI file. Only dated docs and `tools/untangle/move_classes.py` (a skip list).
- Instructions inside are Maven and `curl`: backend-probes/README.md:48-63, 121-126; name-resolution-repro/README.md:15-18; harness/HARNESS.md:3-25 (which points at a `/private/tmp/<a session directory>` scratch directory).
- `legend_rules_test` and `tree_artifact_test` (2026-04-16) are dead prototypes.
- `postgres-dialect/semantics_probe.sh` (2026-10-02) needs a Docker Postgres and the DuckDB CLI. It backs docs/POSTGRES_DIALECT_HOMEWORK_2026_10_01.md. Class B if it is kept.
- Recommendation: class C (attic). At minimum, fix the `.bazelignore` comment.

**repro/**
- 19 upstream bug repros (README plus `.pure` files). Nothing in Bazel reads them.
- Every reproduction instruction is `python3 scripts/corpus/probe_*.py`, all broken because they need cp.txt. Locations: collection-over-tomany:49, dayofyear:46, derived-boolean:51, extends-filter:60, graphfetch-included:63, isempty:36, join-property:67-68, projection-form:52, qualified-property:47, real-column-type:51, regexp-arity:43, registered-but-unusable:49, relation-first-and-last:39, self-join-aggregate:68.
- It also duplicates the concept of `scripts/corpus/repro/`.

**projects/**
- 56 projects. 11 are linked into the stress corpus; 45 are compiled by nothing (see `scripts/projects/check.py` in 1c).
- `CONTRACT.md:3, 72` tells people to run `python3 scripts/projects/check.py`, which is broken.

---

## Section 2 — CI findings

**gates-run.yml**
- **KNOWN K11, now with line numbers:**
  - Lane-to-target map is a jq blob: lines 53-68.
  - Low-memory PCT split in shell: 46-52.
  - Platform exclusions through jq: 69-73.
  - `pip install pyarrow==23.0.1`: 123-128.
  - `GENERATED` lane expanded by a runtime `bazel query`: 144-152.
  - Browser harnesses in a bash loop of `bazel run`: 155-169, selected by `bazel query 'attr(tags,"browser-ci", //datacube:* + //query:*)'`.
  - Install of Chromium through `bazel run //datacube:install_browser`: 131.
- **NEW, high: the GENERATED expansion fails silently.** `bazel query … 2>/dev/null` runs inside `< <(...)` (line 148). A query failure is neither visible nor fatal, so the "checks" lane would go green with none of the `update_generated` diff tests. Fix: a `test_suite` in BUILD.
- **NEW:** `--test_env=PATH` (line 142) passes the runner's PATH into the warehouse tests. That is non-hermetic and makes the test cache key depend on the host. `PYTHONPATH="$RUNNER_TEMP/pyarrow"` and `WAREHOUSE_ARROW_CHECK=required` are test policy that lives in YAML (lines 140-143).
- **NEW:** the `7p` lane exists (line 61), but both lists of valid keys omit it: `gate.yml:33` and `gates-run.yml:80`.
- **NEW:** GATES.md:28 says the browser lane runs "every `//datacube` target tagged `browser-ci`". CI also selects `//query:*` (gates-run.yml:163).

**gate.yml**
- **KNOWN:** actionlint is fetched with `curl` and no checksum (lines 57-60).
- **NEW:** `paths-ignore: "**/*.md"` (lines 23-28) rests on "Prose cannot change a gate's verdict". Two tracked `.md` files are declared test inputs: `core/src/test/resources/bazel_smoke/README.md` (through `_CORE_READS = glob(["src/**"])`, core/BUILD.bazel:266) and `datacube/bench/model/README.md` (through the `portability` glob `bench/**/*`, datacube/BUILD.bazel:94). The claim is false by construction, so a docs-only push can change a test's inputs without CI running.
- **NEW, minor:** every action is pinned by tag (`@v4`), not by SHA.

**diagnostics.yml**
- Clean `bazel test` on one label. It triggers on `tools/oracle-pins.env` (lines 12, 20), the K19 duplicate.

**Not in CI at all (NEW, deepens K1):**
- No workflow runs any `scripts/` ratchet: build.py `--check`, density/executed/stacking/scoreboard `--gate`, parser `mutants.py --check`.
- `docs/gate-additions.patch` (2026-08-16) is an unapplied patch to the Maven-era gate.yml that would have added exactly those, plus an engine-runner stress run. It is a dead artifact.

---

## Section 3 — Documentation findings: every non-Bazel instruction

### Current instructions (not dated history)

**FAQ.md**
- :312-314 build-time table: `mvn clean install -DskipTests`, `mvn clean test -pl engine`, `mvn test -pl engine`.
- :327-328 "How do I run a single test?": `mvn test -pl engine -Dtest=…`, `mvn -pl core test -Dtest="TyperTest"`.
- :318-324 "Why is the build so fast": "minimal Maven overhead"; "No code generation step" is false (`//spec` generators).
- :333-349 project tree still lists `engine/` and `nlq/` (neither exists); NLQ section around 255-303.

**README.md**
- :43-44 "There is no ANTLR in any pom" (no poms exist).
- :65-68 and :242-245 call `engine/` frozen-but-live, contradicting AGENTS.md:72-75 ("engine module is deleted").
- :428-429 "test totals are from surefire reports" plus the `find src/main` derivation; the stats table at :433-437 counts engine tests.
- The build commands themselves (:262-266, :274-275, :413-417) are all `bazel`.

**AGENTS.md**
- :39-40 points to the reference checkouts as `-Dlegend.engine.root` / `-Dlegend.pure.root` (Maven system properties).
- Otherwise there are no build commands; :371-376 correctly retires Maven.

**core/README.md**
- :322 "Run `mvn -pl core test` — `ArchitectureTest` must stay green".
- :22 "Maven: `core/pom.xml` does not declare …" (a wall-enforcement layer that does not exist).

**docs/GATES.md**
- :1 title says "runs ALL of these, sequentially".
- :86-103 (undated heading) "The root flag is a SYSTEM PROPERTY": `tools/allgates.sh`, `mvn` by hand.
- :105-150 (undated heading) "Read this before trusting a green", written as present tense:
  - CI runs through `tools/allgates.sh`, with `.github/actions/gate-env`, `tools/diagnostics.sh`, root `pom.xml` and `tools/oracle-roots.sh`.
  - "`tools/allgates.sh` has no `set -e`".
  - "Core must be INSTALLED … `mvn -pl`".
  - None of these exist. The disclaimer at :8-9 covers this, but readers meet undated, imperative "read this before trusting a green" sections first.
- :65-66 and :1035+ are dated history (fine).

**docs/ENGINEERING_LOG.md** (AGENTS.md:346 lists it as the "Standing tenets" doc)
- :25 `tools/allgates.sh:19-20`.
- :55-67 the build "shape", which says it is "retained because `.github/workflows/gate.yml` cites this file" (gate.yml no longer does): `mvn -pl core clean test`, `mvn -pl core install`, `cd engine && mvn -o test`, `cd ../pct && mvn -o test`.
- :222-226 `mvn -f`.

**docs/RUNNING_THE_CORPUS.md** (2026-08-23; the only "how to run the corpus" doc)
- :14-25 JDK at `~/jdk` and Maven; `mvn -B -pl core install`, `mvn -B -f tools/engine-runner/pom.xml compile` (that pom is gone), `dependency:build-classpath … cp.txt`.
- :40-41 `python3 scripts/corpus/build.py`, `python3 scripts/corpus/run.py`.
- :147 cp.txt.
- :169-172 `python3 scripts/projects/check.py` and `loadtime.py`.

**docs/UPSTREAM_FINDINGS.md** (2026-09-16)
- :11-12 `cd tools/engine-runner && mvn -o compile -q`, `java -cp target/classes:$(cat cp.txt) perf.TestableMain`.
- :23 `mvn -o -pl core test -Dtest=LegendLiteGapTest`.
- :219 cp.txt again.

**docs/UPSTREAM_BOUNDARY_PROGRAM.md** (2026-09-29; "Present tense; no history")
- :13 links the missing `tools/version-report.sh` and `tools/classpath-convergence.sh`.
- :185 "`tools/version-report.sh --check` exits 0 in CI".
- :199, :434, :467, :480, :539 `version-report.sh`.
- :209 `core/pom.xml`.
- :423 `tools/allgates.sh`.

**tools/README-style docs**
- `tools/engine-runner/README.md:36-40`: a `grep | awk` pipeline around `bazel run`, admitted broken at :43-46.
- `tools/census/README.md:10-12, 24-26`: `git switch` plus `tools/census/lanes.sh` / `render.sh`, `python3 tools/census/lanes_diff.py`, `diff`.
- `tools/wrongrows/README.md:13-14`: `bazel-bin/tools/engine-runner/testable …` run directly, not through `bazel run`; :27-28 `--sandbox_writable_path`; :38 `python3 tools/wrongrows/compare.py`.
- `tools/reference/README.md:29-42` "Running it (no build system)": WebStorm JBR `javac` and `java` on `~/legend/engine-dist/...shaded.jar` (marked "kept for ad-hoc joins").
- `tools/oracle-pins.env:24`: `bazel run //tools/bump` (fine).

**scripts/, repro/, projects/, datacube docs**
- `scripts/parser/README.md:28-33` `python3 fixtures.py / keywords.py / mutants.py`; :135-136 `java -cp tools/engine-runner/target/classes:$(cat …/cp.txt) … perf.TokenDump > vocab.tsv`.
- `scripts/corpus/repro/README.md:6-72`: `mvn -o compile`, many `java -cp $CP perf.TestableMain …`.
- `repro/*/README.md`: 14 `python3 scripts/corpus/probe_*.py` instructions (listed in 1e).
- `projects/CONTRACT.md:72` `python3 scripts/projects/check.py <name>`.
- `datacube/bench/README.md:104-107` `npm install`, `node bench/wasm-penalty.mjs`, `node bench/snap-ceiling.mjs`, `node bench/cell-budget.mjs`. The three `.mjs` files are not Bazel targets; they are only scanned by the portability guardrail.
- `datacube/bench/model/README.md:66-69` `duckdb -c ".read bench.sql"`, `python3 widepivot.py`. The 8 `.py` files in `datacube/bench/model` are not built.
- `experiments/*` READMEs (dated; listed in 1e).

**Scripts that generate committed files (header comments)**
- `fixtures/saved-queries/make.mjs:4-6`: `java -jar bazel-bin/core/server_deploy.jar 18777 --query-store <dir> &` then `node fixtures/saved-queries/make.mjs`. Writes the committed `fixtures/saved-queries/*.json`.
- `query/tools/icons.mjs:5`: `node query/tools/icons.mjs <dir>/package > query/src/ui/icons.ts`. Writes the committed generated `query/src/ui/icons.ts`.

### References to scripts that no longer exist, in current text

| missing script | where it is referenced |
|---|---|
| `tools/allgates.sh` | GATES.md :9, :68, :88-103, :107-133; ENGINEERING_LOG.md:25; UPSTREAM_BOUNDARY_PROGRAM.md:423; **parser-equivalence/BUILD.bazel:104** (comment); **core/.../CorpusDifferentialTest.java:21** ("until the generator is wired into tools/allgates.sh"); and 30+ dated docs |
| `tools/diagnostics.sh` | GATES.md:114; **parser-equivalence/BUILD.bazel:92** (comment) |
| `tools/oracle-roots.sh` | GATES.md:126; **tools/native-axes.py:20**; **tools/upstream-drift.py:15** |
| `tools/version-report.sh`, `tools/classpath-convergence.sh` | UPSTREAM_BOUNDARY_PROGRAM.md (above); **tools/upstream-drift.py:52**; GATES.md (dated entries) |
| `tools/bump.sh` | GATES.md and UPSTREAM_BOUNDARY_PROGRAM.md (dated entries only) |
| `tools/corpus-both.sh`, `tools/judge-lanes.sh` | spec/BUILD.bazel:163, 173 — phrased "was …", historical and acceptable |
| `.github/actions/gate-env` | GATES.md:113 |
| `pom.xml` files (root, `pct/`, `parser-equivalence/`, `core/`) | MODULE.bazel comments :97, :137, :150, :178, :196; tools/par/BUILD.bazel:4; core/README.md:22 |

Historical (fine as they stand): docs/BAZEL_DEPENDENCY_PROPOSAL.md and docs/BAZEL_IMPLEMENTATION_PLAN.md both carry a "SUPERSEDED — do not act" banner. Their `mvn` and `python tools/bump_external_versions.py` content (IMPL :273-310, PROPOSAL :619, :1963) is history. The banner, however, points readers to `docs/CORPUS_BURNDOWN_HANDOFF.md` as "current work", which AGENTS.md:350 calls history.

---

## Section 4 — Other NEW findings

1. **`CorpusDifferentialTest` can never run under Bazel.** Its input directory resolves under `TEST_UNDECLARED_OUTPUTS_DIR`, and nothing can pre-populate that. It is a permanently skipped test inside the core suite, and its Javadoc cites the removed `tools/allgates.sh`.
2. **The stress corpus is never run through legend-engine under Bazel.** The README recipe is broken, and the only working path is the macOS-arm64-only `tools/wrongrows/engine-rows.sh`. "Portable expectations" therefore has no gate.
3. **LINKED_PROJECTS exists in three hand-synced copies:** `scripts/corpus/model.py:59`, `StressCorpus.java:26` ("the same list and order as model.py"), and `tools/wrongrows/engine-rows.sh:13`.
4. **Committed `docs/*.tsv` with dead or missing generators are still test inputs.** `//docs:ledgers` (`docs/BUILD.bazel:6-10`) globs every `docs/*.tsv`. That sweeps in:
   - generators that are dead or missing: `ENGINE_FUNCTIONS.tsv` and `ENGINE_SURFACE.tsv` (both say "generated", but no generator exists in the repo), `FUNCTIONS_EXECUTED.tsv` (written by the broken `probe_functions.py`), `SURFACE_BLOCKED.tsv`, `golden-shape-survey-4AD.tsv`;
   - a TSV with no reader at all: `NATIVE_CLAIMS_CENSUS_2026_09_10.tsv`.
   Editing any of them invalidates tests, and none is regenerated or checked.
5. **Generated committed files with no `write_source_files` coverage in my slice:**
   - `scripts/parser/mutants.tsv`
   - `tools/engine-runner/vocab.tsv`
   - `tools/metamodel-census/*.json`
   - `docs/{corpus-coverage.json, census-baseline.json, OUTSTANDING.md, WALL_DEPTH.txt, SCOREBOARD.md}`
   - stress files 59, 60 and 64 (59 and 60 already stale)
   - outside my slice but seen: `fixtures/saved-queries/*.json` (make.mjs) and `query/src/ui/icons.ts` (icons.mjs). Each is either A (wire it) or C (drop the "generated" claim and the generator).
6. **Hard-coded machine paths** in tracked tools:
   - `/Users/<another user>/...`: tools/golden_shape_survey.py:14, tools/metamodel-census/*.py, docs/burndown and invention-audit scripts.
   - `/Users/<user>/legend/legend-lite`: scripts/outstanding.py:14.
   - `~/jdk/jdk-21.0.11+10`: run.py:31, parser/*.py, RUNNING_THE_CORPUS.md.
   - `~/jdk/apache-maven-3.9.9`: census_gate.py:42.
   - `remotejdk25_macos_aarch64`: tools/census/render.sh:9, tools/wrongrows/engine-rows.sh:24.
7. The `.bazelignore` comment ("Two Bazel prototype workspaces") misdescribes what `experiments/` holds.

---

## Section 5 — Coverage statement

**Read in full:**
- scripts/corpus/build.py, scripts/corpus/.gitignore, run.py:1-60, query.py (header and `load`), the load and check parts of model.py.
- The docstring and IO lines of every other `scripts/corpus` module (all 64).
- Header and IO of all `scripts/` top-level and `scripts/parser` / `scripts/projects` files.
- Every `tools/` non-Java file: census/* (all four), ci-watch.sh, oracle-pins.env, tools/BUILD.bazel, bump/BUILD.bazel, reference/README.md, reference/BUILD.bazel, wrongrows/README.md, engine-rows.sh, engine-runner/README.md, engine-runner/BUILD.bazel, engine-runner/.gitignore. Headers and IO for golden_shape_survey, native-axes, scoreboard, upstream-drift, metamodel-census/*, spikes/*, untangle/*, join.py, source_drift.py, compare.py, damage.py.
- All three workflow files.
- README.md and AGENTS.md completely; the FAQ.md build and project sections; GATES.md:1-152, every section heading, and a command grep over all Bazel-era entries (4720-6729).

**Corpus import closure:** computed exhaustively, 27 modules counting build.py. I verified stdlib-only imports and the read and write paths of every closure module by grep. I did not read the bodies of seed.py (21k lines of data), oracle.py and battery.py line by line beyond their IO and nondeterminism checks.

**Command-grepped:** every package README, every docs script (24 plus the TS and Java probes), every experiments/ and repro/ README, docs/BAZEL_*.md, RUNNING_THE_CORPUS.md, UPSTREAM_FINDINGS.md, UPSTREAM_BOUNDARY_PROGRAM.md, ENGINEERING_LOG.md, EXECUTION_PLAN_2026_09_26.md, IN_FLIGHT.md, core/README.md.

**Not covered:** the `.bzl` files under tools/ (left to the other auditors), datacube/demo and query/site `.mjs` coverage beyond the two generators noted, and the ~6,000 lines of dated GATES.md history (grepped for commands, not read).

**Evidence method:** dates via `git log -1 --format=%cs`, references via `git grep`. The only executed code was the in-memory comparison of stress files 59, 60 and 64 against their generators.
