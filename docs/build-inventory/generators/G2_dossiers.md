# G2 dossiers: the relational corpus passes

Repo: `the build/rebuild checkout`, branch `build/rebuild`, HEAD `669b39ad1`. Read-only; only
`bazel query` / `aquery` were run (each one is given below). Paths are repo-relative unless absolute.

Generators (runs/generators/G2.txt): `//spec:judge_host_duckdb`, `//spec:judge_database_duckdb`,
`//spec:judge_host_h2`, `//spec:judge_database_h2`, `//spec:judge_host_warehouse`, `//spec:judge_database_warehouse`.

The six share one macro, one program and almost every fact. Part A sets out the shared facts and answers the
area-specific questions. Part B has the six dossiers, every field, each stating its own values and pointing to Part A
for what is identical. Part C is the table.

---

## Part A. Shared facts

### A1. What one `corpus_lane(...)` expands to

`spec/corpus.bzl:28-156` (the macro). It is called three times: `spec/BUILD.bazel:156-162` (`corpus_duckdb`),
`:169-190` (`corpus_warehouse`, `golden = False`, `tags = ["manual"]`) and `:196-203` (`corpus_h2`,
`jvm_flags = ["-Drcorpus.backend=h2"]`, `memory_mb = 4096`).

Query: `bazel query --output=label_kind 'attr(generator_function, corpus_lane, //spec:*)'` gives 36 targets.

| Target (per lane `<l>`) | Kind | Made at | What it is |
|---|---|---|---|
| `judge_host_<l>` | `_java_run`, testonly | corpus.bzl:72-90 | the host-judge pass (a build action, mnemonic `CorpusHostPass`) |
| `judge_database_<l>` | `_java_run`, testonly | corpus.bzl:91-115 | the database-judge pass (mnemonic `CorpusDatabasePass`); takes `:judge_host_<l>` as a src |
| `corpus_<l>_verdict` | `java_test` (via `junit_test`), small, 256 MB | corpus.bzl:122-136 | `CorpusVerdictTest` over the 4 verdict and log files |
| `update_rcorpus_<l>` | `_write_source_file` umbrella | corpus.bzl:139-150 | golden lanes only; `additional_update_targets` = the five below |
| `update_rcorpus_<l>_0..3` | `_write_source_file` | (write_source_files) | write `<l>-fail-roster`, `-skipped-roster`, `-unordered-register`, `-engine-order-register` from `judge_host_<l>` |
| `update_rcorpus_<l>_4` | `_write_source_file` | (write_source_files) | writes `<l>-database-engine-order-register` from `judge_database_<l>` |
| `update_rcorpus_<l>_0..4_test` | `_diff_test`, small | (write_source_files) | committed copy vs measured file (e.g. `file1 = //spec:judge_host_duckdb/duckdb-fail-roster.txt`, `file2 = //spec:src/test/resources/rcorpus/duckdb-fail-roster.txt`; `bazel query --output=build //spec:update_rcorpus_duckdb_0_test`) |
| `update_rcorpus_<l>_tests` | `test_suite` | (write_source_files) | the five diff tests |
| `corpus_<l>` | `test_suite` | corpus.bzl:152-156 | `corpus_<l>_verdict` + (golden) `update_rcorpus_<l>_tests` |

The duckdb and h2 lanes each have 16 targets: 2 passes, 1 verdict test, 1 umbrella, 5 writers, 5 diff tests, 1 diff
suite and 1 lane suite. The warehouse lane (`golden = False`) has only 4:
`judge_host_warehouse`, `judge_database_warehouse`, `corpus_warehouse_verdict` and `corpus_warehouse`. All four
are `manual`: `bazel query 'attr(tags, "\bmanual\b", attr(generator_function, corpus_lane, //spec:*))'` returns
exactly those four. `bazel query 'tests(//spec:corpus_warehouse)'` still returns `//spec:corpus_warehouse_verdict`,
because a manual test that a suite lists explicitly is kept.

There is also a suite outside the macro: `//spec:judge_lanes` = `corpus_duckdb` + `corpus_h2`
(`spec/BUILD.bazel:286-292`).

### A2. What a pass runs

**The command line.** `java_run` (`tools/java_run/defs.bzl:44-120`) runs
`java @paramfile` with:
- pinned `-Duser.timezone=GMT -Duser.language=en -Duser.country=US -Dfile.encoding=UTF-8`;
- `-Djava.io.tmpdir=<name>_tmp`, a declared output directory (defs.bzl:84-91, 111);
- `-Xmx<memory_mb>`, which is also the action's scheduler `resource_set`, with `cpu: 1` (defs.bzl:19-42, 98, 114);
- the main class `com.legend.tools.junit.JUnitAction` (corpus.bzl:85, 110).

The program arguments are `<verdict> <log> <ledger> [<measured roster>...] -- --select-class=com.legend.rcorpus.MinimalCorpusTest --fail-if-no-tests`
(corpus.bzl:78-80, 98-100). The JVM flags are `_pass_flags`, which are:
- `-Dlegend.engine.root=` and `-Dlegend.pure.root=` (`tools/generators/defs.bzl:24-40`);
- `--enable-native-access=ALL-UNNAMED`;
- `-Dlegend.exec.engineScanOrder=true`;
- `-Drcorpus.measured.out={OUT_DIR}` when golden (corpus.bzl:21-26);
- then `-Dlegend.judge.mode=host|database` and `-Dlegend.judge.ledger={OUT_DIR}/judge-<mode>.tsv`.

The database pass adds:
- `-Dlegend.judge.host.verdict`, `-Dlegend.judge.host.log` and `-Dlegend.judge.ledger.host`, all under `{HOST_DIR}` = the host
  pass's output directory (corpus.bzl:67-68, 104-108);
- `-Drcorpus.host.measured={HOST_DIR}` when golden (corpus.bzl:109).

The warehouse lane adds `-Drcorpus.committed=$(execpath src/test/resources/rcorpus/duckdb-fail-roster.txt)`
(corpus.bzl:71), `-Drcorpus.warehouse.server=$(execpath //warehouse:server_native)` and
`-Drcorpus.warehouse.library=$(execpath //warehouse:duckdb_library)` (spec/BUILD.bazel:172-177).

**JUnitAction** (`tools/junit/JUnitAction.java:45-88`):
- starts heap watching (`:31`);
- points `legend.outputs.dir` at a fresh `Files.createTempDirectory("junit-action-reports-")` under the action's tmpdir
  (`:55-58`);
- redirects stdout and stderr to `<log>` (`:62-64`);
- runs `JUnitMain.run(...)` in process, with no Bazel test protocol (`:71`; `tools/junit/JUnitMain.java:149-212`);
- writes a one-line `UNMEASURED: ...` file for every declared output the run did not write (`:79-85`);
- writes the JUnit exit code to `<verdict>` (`:86`).

The JVM exits 0 when the code is 0 or 1. So a pass whose tests FAILED is a successful, cached action. Any other code (no
tests found, runner error) fails the action (`:39`). Consequence: a red or flaky pass is cached until an input changes,
in the local and the CI disk cache. `--nocache_test_results` does not rerun it. The P3-01 amendment in the workplan
says so (BAZEL_FIRST_CLASS_WORKPLAN:1587-1593).

**The test it selects:** `MinimalCorpusTest.corpus()` (`spec/src/test/java/com/legend/rcorpus/MinimalCorpusTest.java:125-137`),
tagged `@Tag("heavy")` (`:42`). It:
1. builds `MinimalCorpus`, which assembles legend-engine's `core_relational` test corpus as one model
   (`Corpus.java:48-58`, `MinimalCorpus.java:227-248, 360-370, 418-440`);
2. pins the census against `ratchets.tsv` and an independent text scan (`:155`, `:856-870`);
3. runs every discovered test (2,613 by `spec/src/test/resources/com/legend/generators/ratchets.tsv`
   `corpus.census.discovered`) through the platform's `PureTestRunner` (`:185-260`);
4. writes side reports, the measured rosters, then checks:
   - the fail, skipped, accepted, ord and unordered rosters and the engine-order register (`:371-430`);
   - in database mode, the outside-body and host-compared registers (`:431-446`);
   - the channel ceilings, the strength floors and, in database mode, the per-assert differential (`:447-449`, `:458-524`).

**"Host" vs "database" judge.** This is the run's assert judge, `com.legend.ExecuteOptions.JudgeMode` (`core/src/main/java/com/legend/ExecuteOptions.java:23-29`):
- HOST is "the verdict of record, judged in Java over fetched values".
- DATABASE means "both sides planned and the database returns the verdict row (VerdictSql)".

`MinimalCorpus` reads `legend.judge.mode` once and hands it to the runner (`MinimalCorpus.java:336-345`). The database
pass:
- refuses to run unless the host verdict is `0` (`MinimalCorpusTest.java:132-135`, `JudgeLedger.java:61-81`);
- holds its fail list against the host pass's measured fail roster, with LOST and GAINED pinned to the policy
  registers `<l>-database-{lost,gained}-register.txt` (`:899-958`);
- joins the two ledgers per assert (`JudgeLedger.diff`, `JudgeLedger.java:173-198`), with unregistered
  disagreements at zero and unjudged asserts under `<l>-judge-unjudged-ceiling.txt` (`MinimalCorpusTest.java:458-524`).

**What it executes:**

| Lane | Database the corpus runs on | Referee | Extra process |
|---|---|---|---|
| duckdb | in-process DuckDB: JDBC `jdbc:duckdb:`, one root connection, `ATTACH ':memory:' AS __ws_N` per workspace, `SET threads=1` (`DuckWorkspaces.java:90-127`); DuckDB JDBC 1.4.4.0 (aquery input `duckdb_jdbc-1.4.4.0.jar`) | an in-memory H2 mirror per session (`MinimalCorpus.java:559-567`) | none |
| h2 | a fresh in-memory H2 per session, `jdbc:h2:mem:c2s<N>` + the engine's settings and extension aliases (`MinimalCorpus.java:480-498`); H2 2.1.214 (aquery input) | the same session ("oracle=same-session", `MinimalCorpusTest.java:985`) | none |
| warehouse | the duckdb path, but every connection is `jdbc:warehouse:http://127.0.0.1:<port>/main` | H2 mirror as duckdb | **starts `//warehouse:server_native`** (GraalVM native image, DuckDB 1.5.5.1) as a child process: `--port 0 --data <tmp> --user rcorpus:rcorpus --owner rcorpus --concurrency 4 [--duckdb-library <lib>]`, reads its "warehouse listening on" line, and destroys it in a shutdown hook (`DuckWorkspaces.java:137-185`). The data directory is created under the action's tmpdir, so it lands in the declared `_tmp` output. |

**What a pass writes** (aquery outputs: `bazel aquery "mnemonic('Corpus.*', //spec:<t>)" --output=jsonproto`):

| File | Writer | Content |
|---|---|---|
| `verdict.txt` | JUnitAction:86 | JUnit exit code, one line (`0` = every selected test passed) |
| `host.log` / `database.log` | JUnitAction:62-77 | the run's stdout and stderr: `[corpus2] ...` census lines, `FAIL`/`SKIP` rows, JUnit's summary and failures, `[bazel] heap: live peak ...`, the side-report path; the warehouse lane also gets `[warehouse] ...` server stderr |
| `judge-host.tsv` / `judge-database.tsv` | `JudgeLedger.record` (`JudgeLedger.java:84-104`) | per adjudicated assert: `test \t ordinal \t assert family \t PASS|FAIL|UNJUDGED` |
| host, golden only: `<l>-fail-roster.txt`, `<l>-skipped-roster.txt`, `<l>-unordered-register.txt`, `<l>-engine-order-register.txt` | `writeMeasured` / `writeRoster` (`MinimalCorpusTest.java:1152-1184`) | sorted unique test names (registers prefixed `unordered-chain ` / `engine-order `), LF |
| database, golden only: `<l>-database-engine-order-register.txt` | same, database branch (`:1159-1161`) | same shape |
| `<name>_tmp/` (tree; an output, but not in DefaultInfo: defs.bzl:111, 117) | JUnitAction:56, JUnitMain's `junit-xml` temp dir, MinimalCorpusTest:264-285, 432, 440, DuckDB JDBC's native library extraction, the warehouse data dir | `junit-action-reports-<random>/corpus2-{pass,fail,skipped,engine-order,elapsed}.txt`, `corpus2-{statement-origins,body-shapes,fallbacks}.tsv`, database mode: `corpus2-{outside-body,host-compared}.txt`; JUnit XML; warehouse: the server's data. OPEN: the exact tree contents were not observed (no built outputs exist in any checkout); settle with `ls -R bazel-bin/spec/judge_host_duckdb_tmp` after a build. |

**What the verdict test then checks.** `corpus_<l>_verdict` runs `CorpusVerdictTest.bothPassesPassed`
(`spec/src/test/java/com/legend/rcorpus/CorpusVerdictTest.java:18-22`). It calls `JudgeLedger.requireHostPassed()`, then
`requirePassed("database-judge", ...)`. Each reads a verdict file through `-D...=$(rlocationpath ...)`
(corpus.bzl:127-132). If the code is not `0`, it throws an AssertionError quoting up to 300 lines of that pass's log
from its `Failures (` line (`JudgeLedger.java:61-76`). It checks nothing else: the rosters are checked by the diff
tests and the policy files by the passes themselves. The verdict test's classpath is the whole `spec_tests_lib`
(corpus.bzl:133), so it reruns on any core or spec change, and on any rerun of a pass (its data includes the logs,
which are not byte-stable: A6).

### A3. Inputs, declared vs read (all six)

From the aquery dumps (the summariser is in the scratchpad; the counts are of the expanded input depsets):

| Group | host duckdb/h2 | database duckdb/h2 | host warehouse | database warehouse |
|---|---|---|---|---|
| total inputs | 17,180 | 17,187 | 17,189 | 17,192 |
| legend-engine tree (`@legend_engine_src//:tree` + pom.xml) | 12,900 | 12,900 | 12,900 | 12,900 |
| legend-pure tree | 2,693 | 2,693 | 2,693 | 2,693 |
| `//core:srcs` (all of core `src/**`) | 1,333 | 1,333 | 1,333 | 1,333 |
| `:corpus_srcs` (spec `src/**` minus the 10 measured files: 11 gen java, 36 test java, 20 rcorpus policy, `ratchets.tsv`, 2 reference-lane) | 70 | 70 | 70 + 5 committed DuckDB rosters = 75 | 75 |
| host-pass outputs | - | 7 (verdict, log, ledger, 4 rosters) | - | 3 (verdict, log, ledger) |
| JDK runtime files | 117 | 117 | 117 | 117 |
| jars: core | 33 (`builtin` ... `values`, incl. `server_lib`, `ide`, `probe`, `testdatagen`) | 33 | 33 | 33 |
| jars: other first-party | `base`, `json`, `testing`, `tools/junit`, spec `claims`/`generators`/`source_tree`/`spec_tests_lib`, 2 rules_java runfiles | same | same + `warehouse/client`, `warehouse/sqlapi` | same |
| jars: Maven | 19 `@maven_test` (JUnit 5 platform, vintage engine, archunit x5, opentest4j, apiguardian, slf4j-api) | same | same | same |
| jars: product drivers | duckdb_jdbc 1.4.4.0, h2 2.1.214, postgresql 42.7.13, sqlite-jdbc 3.47.1.0 | same | same | same |
| other | - | - | `server_native-bin`, `libduckdb_java.so_osx_universal` | same |

The classpath comes from `deps = runtime_deps + ["//tools/junit"]` = `//spec:spec_tests_lib` (+ `//warehouse:client`).
`spec_tests_lib` (`spec/BUILD.bazel:88-113`) is every spec test class and every spec resource but the measured rosters
(`:94`), plus `//core` (the umbrella), `//core:drivers`, `//core:shadow_binding`, `//testing`, archunit and the JUnit
APIs.

**Actually read** (from the source):
- **legend-engine tree:**
  - `Corpus.RELATIONAL` (`.../legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational`,
    `spec/src/gen/java/com/legend/generators/UpstreamFiles.java:19-22`), walked whole (`MinimalCorpus.java:418-425`,
    `MinimalCorpusTest.java:872-896`);
  - `Corpus.M2M_TESTS` and `MinimalCorpus.GRAPH_FETCH_DOMAIN`, walked (`MinimalCorpus.java:427-440`);
  - the named `LIBRARY_FILES` and `SHAPE_FILES` (`UpstreamFiles.java:34-75`, `MinimalCorpus.java:247-255, 360-370`);
  - test resources resolved under `RELATIONAL/../..` (`MinimalCorpus.java:599-603`).
- **Classpath resources:** the 20 rcorpus policy files (`getResourceAsStream` at `MinimalCorpusTest.java:508, 1074, 1092, 1199`) and `ratchets.tsv`
  (`SpecRatchets.java:57-80`).
- **Files named by flags:** the host outputs (database pass), and in the warehouse lane the committed DuckDB rosters
  through `ProgramPaths.rootOf("rcorpus.committed")` (`MinimalCorpusTest.java:1189-1199`). The warehouse lane also
  starts the server binary and passes the library path on.
- **System properties:** those listed in A2, plus `rcorpus.test` (scope) and `rcorpus.warehouse.data`
  (`DuckWorkspaces.java:143`); neither is set by the BUILD. No environment variable is read: `TestOutputs` checks
  `TEST_UNDECLARED_OUTPUTS_DIR`, which an action does not have.

**Over-declared** (declared, never read in the pass's code path):
- `//core:srcs`: 1,333 files. No class on the corpus path opens a core source file; the only `Files.read*` in
  `core/src/main/java` is `server/SavedQueries.java`.
- `:corpus_srcs` as files: 70. The policy files are read from the classpath jar, not from the tree.
- the legend-pure tree: 2,693 files. Only other spec classes read `legend.pure.root`; `Corpus.java:83-86` says "the
  corpus lane reads NO legend-pure sources".
- most of the 12,900 engine files.
- the classpath beyond what MinimalCorpus needs: the 11 generator classes, the ~35 other spec tests, archunit, the
  postgresql/sqlite drivers and, probably, `core:server_lib`. Proof that core tests reach the pass:
  `bazel query 'somepath(//spec:judge_host_duckdb, //core:src/test/java/com/legend/ArchitectureTest.java)'` gives
  `judge_host_duckdb -> //core:srcs -> ArchitectureTest.java`.

The area-2 audit raised the same over-declaration (area2_report.md:112). Verified here.

**Under-declared:** none found. OPEN: whether DuckDB autoloads or installs an extension (from `~/.duckdb` or the
network) during the corpus. Nothing in the repo sets `autoinstall`/`autoload` (`git grep` empty). Settle it by
running a pass with `--sandbox_default_allow_network=false` and grepping the log, or by checking for an
`~/.duckdb/extensions` access.

### A4. Why the warehouse lane is manual and the others are not

- `tags = ["manual"]` sits on the `corpus_warehouse` call (`spec/BUILD.bazel:189`). The macro passes `tags` to every
  target it makes (corpus.bzl:89, 114, 135, 147, 155). The duckdb and h2 calls pass none.
- The reason in the code: "Not in the chain yet: its differences from :corpus_duckdb are the leg's rows"
  (`spec/BUILD.bazel:164-168`). The lane was manual from birth (`3d549bc1d`, 2026-09-26, "W1c": then a `junit_test`
  with `tags = ["manual"]`). The workplan's A22 restates it: "`corpus_warehouse` stays `manual` (it needs the native
  binary and the whole corpus) and joins P5-08's weekly `//gates:heavy` suite" (BAZEL_FIRST_CLASS_WORKPLAN:1841, 1846).
- P5-08 (weekly `heavy.yml`, :2473-2485) is not done. `git grep heavy.yml` and `gates/BUILD.bazel` have no
  `heavy` suite. **Today nothing automatic runs the warehouse lane.** The last recorded runs: `a03bc63ec` ("the three
  corpus lanes ... pass") and docs/GATES.md:6749.

**What pulls the duckdb and h2 passes into `bazel build //...`.** First, the four passes are non-manual targets
themselves, so `//...` builds them directly. Then every non-manual target that reaches them
(`bazel query 'rdeps(//..., //spec:judge_host_duckdb + //spec:judge_host_h2 + //spec:judge_database_duckdb + //spec:judge_database_h2) except attr(tags, "\bmanual\b", //...)'`):

- **Building:** `corpus_<l>_verdict` (data). Path: `somepath(//spec:corpus_duckdb_verdict, ...)` =
  `corpus_duckdb_verdict -> judge_host_duckdb/verdict.txt -> judge_host_duckdb`.
- **Building:** `update_rcorpus_<l>_{0..4}_test` (`file1`). Path: `update_rcorpus_duckdb_4_test ->
  judge_database_duckdb/duckdb-database-engine-order-register.txt -> judge_database_duckdb -> judge_host_duckdb`.
- **Building:** `update_rcorpus_<l>_{0..4}` (`in_file`, in the executable's runfiles) and the umbrella
  `update_rcorpus_<l>`. Path: `update_rcorpus_h2 -> update_rcorpus_h2_0 -> judge_host_h2/h2-fail-roster.txt ->
  judge_host_h2`.
- **Expanded, not built** (suites): `corpus_<l>`, `update_rcorpus_<l>_tests`, `judge_lanes`.
- **Analysis-only reach:** `//gates:local`, `//tools/guards:classpath_test`, `markdown_inputs_test`, and
  `{spec,tools/guards}:guard_{classpaths,markdown}`, `classpath_reports`, `markdown_reports`.
  - These reach the passes in the query graph, the warehouse passes too (same rdeps query on the warehouse passes).
  - But `classpath_report` reads only `JavaRuntimeClasspathInfo` and `markdown_report` only runfiles lists. Both write
    with `ctx.actions.write` (`tools/guards/classpath.bzl:38-50`, `markdown.bzl:12-18`).
  - Proof that nothing in the local gate consumes a pass output:
    `bazel aquery 'inputs(".*spec/judge_(host|database)_.*", deps(//gates:local))'` gives "No actions matched". The
    same regex over `deps(//spec:judge_database_duckdb + //spec:corpus_duckdb_verdict)` matches 2 actions (a control).

### A5. CI: lanes 4, 5 and build, and what is uploaded

- **Platforms.** `gate.yml` calls `gates-run.yml` for linux (ubuntu-latest), macos (macos-14, `low-memory: true` gives
  `--config=ci-small`) and windows (windows-2022). It runs every lane except `browser` off Linux. linux-arm runs only
  `native` (gate.yml:65-101).
- **Triggers.** Push to main and pull requests (not `**/*.md`), and `workflow_dispatch` (gate.yml:18-40).
- **Lane 4,** "gates 4+11 DuckDB corpus, both judges": `bazel test --config=ci [--config=ci-small] //spec:corpus_duckdb`
  (gates-run.yml:53, 152-164). That runs `corpus_duckdb_verdict` + 5 diff tests (`tests(//spec:corpus_duckdb)`), so it
  builds `judge_host_duckdb` then `judge_database_duckdb`.
- **Lane 5,** "gate 5 H2 corpus, both judges": `//spec:corpus_h2`, the same with h2.
- **Lane build:** `bazel build //...` (gates-run.yml:63, 163). It builds all four duckdb and h2 passes again (A4).
  - Each lane has its own cache key (`...-${{ matrix.lane.key }}`, gates-run.yml:119-122), so the build lane and lanes
    4/5 normally run the corpus separately. Each CI run computes each pass up to twice per platform, so 6 times.
  - `--config=ci-small` only sets `--local_test_jobs=1` (.bazelrc:70). It does not limit build actions, so on the 7 GB
    macOS runner the 4 GB H2 pass is scheduled by its `resource_set` alone, beside the build's other actions.
- **What is uploaded.** The "test logs" step (`if: always()`, gates-run.yml:196-211) uploads, besides the test logs,
  XML and `test.outputs`:
  - `bazel-bin/spec/judge_host_*/host.log`, `.../verdict.txt`;
  - `bazel-bin/spec/judge_database_*/database.log`, `.../verdict.txt`;
  - `bazel-bin/spec/judge_*/*-*.txt`, which are the measured rosters.

  They go into artifact `<platform>-lane-<key>`, in every lane where they exist (4, 5, build). Not uploaded: the
  ledgers (`*.tsv`) and the `_tmp` side reports, which are one directory deeper. OPEN: with Bazel 9.2's default output
  download mode and a disk-cache hit, an output no action consumes (e.g. `judge-database.tsv`) may not be present
  under `bazel-bin`. Settle it by listing a lane-4 artifact.
- **Who reads the upload: humans.**
  - The commit that added the rosters to it says "CI uploads the measured rosters, so a diff seen in CI can be read and
    blessed from them" (`77f4107c1`).
  - docs/GATES.md:74-77 ("Reading a result") points readers at `bazel-bin/spec/judge_host_<lane>/` and
    `judge_database_<lane>/`.
  - No workflow step or program downloads the artifact (`git grep` for the artifact names finds only gates-run.yml).
- **Timeout.** A pass is a build action, so no Bazel test timeout applies. A hung pass is stopped only by the job's
  `timeout-minutes: 90` (gates-run.yml:86). The code comment at `MinimalCorpusTest.java:362-363` ("a runaway is the
  lane's Bazel timeout") is stale since P3-01.

### A6. Determinism (all six)

- **`verdict.txt`, the rosters, the ledger: deterministic given the code.**
  - The rosters are a `TreeSet` written with LF (`MinimalCorpusTest.java:1175-1184`; LF made explicit in `77f4107c1`).
  - The ledger is in statement order over a corpus sorted by a separator-normalised path (`MinimalCorpus.java:410-425`,
    "Windows's Path.compareTo is CASE-INSENSITIVE").
  - P3-01's proof (`c1307ee81`): "both ledgers ... identical to the last prerun-era run, row for row".
  - One committed copy is diff-tested on three platforms, so the measurement must be platform-independent. OPEN:
    settle it by a green lane 4/5 on all three platforms since `77f4107c1`.
- **`host.log` / `database.log`: not deterministic.** They hold JUnit's "finished after N ms", `[corpus2] ... in Ns`,
  the `slow <ms>` lines (`MinimalCorpusTest.java:337-361`), heap peaks (`JUnitMain.java:462-471`) and the random
  side-report path (`JUnitAction.java:67`).
- **`_tmp/`: not deterministic.** It holds random directory names and elapsed times.
- **Effect.** Consumers of the logs (the database pass, the verdict test) get no early cutoff. The diff tests do,
  because they consume only rosters.

### A7. The rosters: expected results re-blessed on purpose

- **What is measured.** The 10 measured files (5 per golden lane) are the engine's corpus behaviour: which tests fail,
  are skipped, compare unordered, or lean on the test-lane scan order. Classified in `e8dcffb38` and in the workplan's
  P2-15 amendment (:1458-1468). The other 20 rcorpus files are hand-owned POLICY.
- **Not inputs.** The measured files are excluded from `corpus_srcs` and the classpath (`spec/BUILD.bazel:13-28, 94`),
  so a re-bless never reruns the corpus. They are inputs only to the diff tests and, as DuckDB's copies, to the
  warehouse passes.
- **Re-blessed deliberately.** "the corpus rosters ... are re-blessed only deliberately, lane by lane, never as a side
  effect of regenerating something else: a newly failing test must be a decision" (`BUILD.bazel:102-104`; `77f4107c1`
  took them out of `//:update_generated`). So they are expected results that a human re-blesses on purpose, after
  reading the diff.
- **Who is told to run the writers, and where:**
  - the diff test's failure message: "bazel run //spec:update_rcorpus_<lane>, and give the reason in the commit (a test
    that newly fails is a regression to fix or a policy row to add, never a roster line to accept silently)"
    (corpus.bzl:142);
  - docs/GATES.md:76-77;
  - the comments at BUILD.bazel:102-104, `MinimalCorpusTest.java:47-51` and `CorpusVerdictTest.java:12-13`.

  They run locally: the writers rebuild the passes (H2 at 4 GB), or the files are taken from the CI artifact (A5). The
  bump does not run them (A8).
- **Inconsistencies found:**
  1. corpus.bzl:148-149 still says "//:update_generated runs it with every other generated file", and keeps
     `visibility = ["//:__pkg__"]` for that. Both have been stale since `77f4107c1`.
  2. The diff message asks for the reason "in the commit". The pass's own LOST/GAINED messages say "in docs/GATES.md"
     (`MinimalCorpusTest.java:970`, `1059`, `1138`).
  3. docs/GATES.md:21 calls gate 4 "host judge", but the lane runs both judges.
  4. Both H2 engine-order files are empty (0 bytes; `ls -la`): `legend.exec.engineScanOrder` is read only by DuckDB's
     pass list (`core/src/test/java/com/legend/TestLaneOrderGuardrailTest.java:78-79`). So `update_rcorpus_h2_3/_4`
     maintain files that are empty by construction.

### A8. Who runs them today (summary)

| Runner | duckdb passes | h2 passes | warehouse passes |
|---|---|---|---|
| `bazel build //...` | yes: non-manual targets, also via the verdict test, diff tests and writers (A4) | yes | no (manual; only analysis-only reach) |
| `//gates:local` | no: CI-only by design (gates/BUILD.bazel:3-5); aquery shows no consuming action | no | no |
| `//:generated` | no (its suite list, BUILD.bazel:79-97, has no spec rcorpus suite) | no | no |
| `//:update_generated` | no (BUILD.bazel:102-124) | no | no |
| CI lane 4 / 5 / build | lane 4 + build, on linux, macos, windows | lane 5 + build, on linux, macos, windows | none |
| bump (`tools/bump/Bump.java:150-153`) | phase 3 `bazel test //...` runs `corpus_duckdb_verdict` and the diff tests, so the passes | same | no (manual) |
| by hand | `bazel test //spec:judge_lanes` (README.md:444, GATES.md:59); writers per A7; `//spec:corpus_one` (a separate binary, BUILD.bazel:205-227, not the action) | same | `bazel test //spec:corpus_warehouse` (docs/WAREHOUSE_W1_DESIGN_2026_09_26.md:208) |

### A9. The recommendation, shared reasoning

All six are **TEST-IN-DISGUISE**:
- Their product is a verdict on engine behaviour (`verdict.txt`, plus the assertions inside the pass).
- For the golden lanes, it is also a set of goldens of engine behaviour (the measured rosters), which a human re-blesses.
- The roster history shows feature commits moving them (e.g. `439805f53`, `2747bd1a4` on the fail rosters). Their true
  trigger is engine behaviour, not a file of ours that a generator transcribes.
- Being build actions has a cost: no test timeout, no `--runs_per_test`, failures cached as successes, and the corpus
  runs inside `bazel build //...`. These are the R3 and R4 causes in BUILD_REBUILD_DESIGN_2026_10_05.md:107-118.

The constraint that made them actions still holds: the database pass reads the host pass's ledger and rosters, and the
diff tests need the rosters as build outputs (P3-01, P2-15). So the cheapest correct shape keeps the actions but:

- **(a) Tag every corpus_lane target `manual`.** area2_report.md:240-241 proposes a similar fix: one shared tag (e.g.
  `corpus`) that the build lane excludes. The lane suites still run manual targets,
  because an explicit suite list keeps manual tests (A1). Lanes 4 and 5 and the bump then name `//spec:corpus_<lane>`
  explicitly. `bazel test //...` would stop running the corpus, so the bump's phase 3 must name the two suites.
- **(b) Narrow the declared inputs to what is read:**
  - drop `//core:srcs`, `:corpus_srcs` and the legend-pure tree;
  - make the engine tree a filegroup of `core_relational` + the core-pure subtrees `UpstreamFiles` names;
  - give the pass a corpus-only library (rcorpus + harness + `SpecRatchets`) instead of `spec_tests_lib`;
  - depend on the core libraries the runner links instead of the `//core` umbrella (OPEN: the exact set; settle with a
    `jdeps` of the rcorpus/harness classes).

  Then a pass reruns on core main code, the corpus harness, the policy files, the backend driver and the upstream pin,
  and nothing else.
- **(c) Keep the writers out of `//:update_generated` and out of a bump-only update.** They stay a deliberate, per-lane
  human action. The bump's "judgement half" text (Bump.java:160-167) should name them.
- **(d) Run the diff tests where the passes run:** their own lanes (4/5) and the bump's check. Not the everyday gate.
- **Open design point for the user.** Whether the verdict test could become the database pass itself (a real
  `junit_test` reading the host action's outputs) loses the database register's diff test. Today that register needs a
  build output. That is a decision, not a fact.

---

## Part B. The dossiers

### B1. `//spec:judge_host_duckdb`

1. **Identity.**
   - Label `//spec:judge_host_duckdb`; rule `_java_run` (testonly), made by macro `corpus_lane` at
     `spec/BUILD.bazel:156-162` (body at `spec/corpus.bzl:72-90`).
   - Program: `com.legend.tools.junit.JUnitAction` (`tools/junit/JUnitAction.java:29`, run at `:45`), selecting
     `com.legend.rcorpus.MinimalCorpusTest#corpus` (`spec/src/test/java/com/legend/rcorpus/MinimalCorpusTest.java:125`).
2. **What it computes.** It runs all 2,613 legend-engine `core_relational` corpus tests through the platform against
   in-process DuckDB, with every assert judged in Java (the host judge, the verdict of record). It records:
   - every assert's verdict (the ledger);
   - which tests fail, are skipped, compare as multisets (no sort), or were changed by the test-lane scan-order
     emulation (the four measured rosters);
   - whether every hand-owned policy pin held (the verdict).
3. **Why it exists.**
   - Commit `c1307ee81` (P3-01, 2026-10-05): the host pass became an action so its ledger feeds the database pass
     instead of a child JVM the test started.
   - Commit `e8dcffb38` (P2-15): it writes the measured rosters, which are diff-tested.
   - It protects Maven gate 4 (docs/GATES.md:21): the engine's behaviour on its own relational corpus.
   - The reason still holds.
4. **Inputs, declared** (aquery, A3): 17,180 inputs. Our sources: `//core:srcs` 1,333 + `:corpus_srcs` 70.
   Upstream: engine tree 12,900, pure tree 2,693. Jars: 33 core + 10 other first-party/Bazel + 19 `@maven_test` +
   4 product drivers (66). The JDK: 117. No other generator's output.
5. **Inputs, actually read.** A3: the relational corpus and named engine files, the 20 policy resources and
   `ratchets.tsv` from the classpath, and the properties in A2. Over-declared: `//core:srcs`, `:corpus_srcs` (as files),
   the pure tree, most of the engine tree, and the classpath excess. Under-declared: none found (DuckDB extension
   question OPEN).
6. **Outputs.**
   - `judge_host_duckdb/judge-host.tsv`: the per-assert ledger.
   - `verdict.txt`: the JUnit exit code.
   - `host.log`: the run log.
   - `duckdb-fail-roster.txt`: tests that fail.
   - `duckdb-skipped-roster.txt`: tests that reach no verdict.
   - `duckdb-unordered-register.txt`: `unordered-chain <test>` rows.
   - `duckdb-engine-order-register.txt`: `engine-order <test>` rows.
   - `judge_host_duckdb_tmp/`: side reports and scratch.
7. **Committed?**
   - The 4 rosters are committed at `spec/src/test/resources/rcorpus/duckdb-{fail-roster,skipped-roster,unordered-register,engine-order-register}.txt`
     (107 / 14 / 921 / 993 lines).
   - Writers: `//spec:update_rcorpus_duckdb_0..3`, umbrella `//spec:update_rcorpus_duckdb`. Diff tests:
     `update_rcorpus_duckdb_0..3_test`, in suite `update_rcorpus_duckdb_tests`, in `//spec:corpus_duckdb`.
   - Not in `//:generated` or `//:update_generated` (BUILD.bazel:102-104).
   - The ledger, verdict and log are not committed. They are build outputs consumed as below.
8. **Who consumes it.**
   - `//spec:judge_database_duckdb` takes all 7 files: verdict, log, ledger and rosters (corpus.bzl:94, 104-109).
   - `//spec:corpus_duckdb_verdict` takes verdict.txt and host.log.
   - The writers and diff tests `_0..3` take the rosters.
   - The CI artifact gets host.log, verdict.txt and the rosters (gates-run.yml:205-209), read by humans (A5).
   - Docs: GATES.md:21, 74-77; README.md:444.
   - No product code reads these files (`git grep` for `judge-host.tsv`, `fail-roster` and `engine-order-register`:
     only spec, docs, and a comment in `core/.../StableScanOrder.java:25`).
9. **Determinism.** A6: the rosters, ledger and verdict are deterministic (sorted, LF; ledger row-for-row equal per
   `c1307ee81`). host.log and `_tmp/` are not.
10. **Cost.**
    - `memory_mb = 1024` (java_run's smallest size), which is the `-Xmx` and the `resource_set` (cpu 1) (spec/BUILD.bazel:155-158).
    - One JVM; in-process DuckDB plus H2 mirrors; no server.
    - The whole corpus: about 38 s per pass on the desk. The prerun-era lane, both passes, measured 75.7-78.4 s
      (docs/GATES.md:6085, 6238). Heap peak 651 MB / live 243 MB for both passes in one JVM then (GATES.md:6443).
11. **What reruns it today.** Any change to:
    - core main code (33 jars) or any core file at all, tests included (`//core:srcs`; `somepath` in A3);
    - any spec test, generator or resource but the 10 measured files (`spec_tests_lib`: e.g.
      `somepath(//spec:judge_host_duckdb, //spec:src/test/java/com/legend/generators/PreludeGeneratorTest.java)` =
      `-> //spec:spec_tests_lib -> PreludeGeneratorTest.java`, and the reference-lane golden the same way);
    - `//core:server_lib` (`-> spec_tests_lib -> //core:core -> //core:server_lib`) and `//core:drivers`;
    - base, json, testing, tools/junit;
    - the upstream pins, both trees (`somepath(//spec:judge_host_duckdb, @legend_pure_src//:pom.xml)` is direct);
    - the JDK and the `@maven_test` pins.
12. **What SHOULD rerun it.** Engine behaviour: core main code on the corpus path, the rcorpus/harness code, the
    policy files, the DuckDB/H2 driver versions, and an upstream bump (the corpus itself).
13. **Who runs it today.** A8: `bazel build //...` (non-manual); CI lane 4 and the build lane on linux, macos and
    windows; the bump's phase 3 `bazel test //...`; humans via `judge_lanes` and the writers. Not the local gate.
14. **Recommendation.** **TEST-IN-DISGUISE.**
    - **Manual?** Yes; run only through `//spec:corpus_duckdb` (explicit suite membership keeps it).
    - **Writers:** neither `//:update_generated` nor a bump-only update. A deliberate human re-bless per lane (A7, A9c).
    - **Diff tests:** lane 4 and the bump's check, not the everyday gate.
    - **Narrowing:** A9b.
15. **Open questions.** DuckDB extension autoload (A3); the `_tmp` contents (A2); the exact core libraries needed
    (A9b); BwoB and the upload (A5).

### B2. `//spec:judge_database_duckdb`

1. **Identity.** `//spec:judge_database_duckdb`; `_java_run` (testonly), from `corpus_lane` at
   `spec/BUILD.bazel:156-162` (body `spec/corpus.bzl:91-115`). Program: JUnitAction over `MinimalCorpusTest#corpus` with
   `-Dlegend.judge.mode=database`.
2. **What it computes.** The same corpus on in-process DuckDB, but each assert's verdict comes back from the database
   as a verdict row (VerdictSql). It then:
   - joins its per-assert ledger to the host pass's, requiring zero unregistered disagreements and unjudged asserts
     within the ceiling (`MinimalCorpusTest.java:458-524`);
   - holds its fail list against the host's measured fail roster through the lost and gained registers;
   - pins the outside-body and host-compared registers;
   - measures its own engine-order register.
3. **Why it exists.**
   - Gate 11 (docs/GATES.md:29), the judging-two-modes program (docs/DATABASE_MODE_HOMEWORK_2026_09_18.md).
   - An action since `e8dcffb38` (P2-15), which also found that this register had never been checked: 52 stale rows.
   - Still needed: it is the only check of the database judge on the corpus.
4. **Inputs, declared.** 17,187 = the host pass's 17,180 + 7 host outputs (aquery). 33 core jars.
5. **Inputs, actually read.** As B1, plus:
   - `{HOST_DIR}/verdict.txt` and `host.log` (`JudgeLedger.requireHostPassed`);
   - `judge-host.tsv` (`legend.judge.ledger.host`);
   - the 4 host rosters (`rcorpus.host.measured`; `readRoster` reads all four, `MinimalCorpusTest.java:899-958, 1189-1199`);
   - the database-only policy files: `duckdb-database-{accepted,lost,gained,differential,untriaged,outside-body,host-compared}-register.txt`
     and `duckdb-judge-unjudged-ceiling.txt`.

   Over-declared as B1. Under-declared: none found.
6. **Outputs.**
   - `judge-database.tsv`: the per-assert ledger.
   - `verdict.txt`.
   - `database.log`.
   - `duckdb-database-engine-order-register.txt`: 885 lines committed.
   - `judge_database_duckdb_tmp/`.
7. **Committed?**
   - The register: `spec/src/test/resources/rcorpus/duckdb-database-engine-order-register.txt`; writer
     `update_rcorpus_duckdb_4`; diff test `update_rcorpus_duckdb_4_test`, in `update_rcorpus_duckdb_tests`, in
     `corpus_duckdb`. Not in `//:generated` or `//:update_generated`.
   - The rest is not committed.
8. **Who consumes it.**
   - `corpus_duckdb_verdict` takes verdict and log.
   - `update_rcorpus_duckdb_4` and `_4_test` take the register.
   - The CI upload gets database.log, verdict.txt and the register.
   - `judge-database.tsv` has **no consumer**: not uploaded (`*.tsv`), no target reads it. Only GATES.md:76 tells
     humans it is in `bazel-bin`.
9. **Determinism.** As B1 (A6).
10. **Cost.** 1024 MB; one JVM; in-process DuckDB plus H2 mirrors. It starts only after the host pass (a src). The
    whole corpus again: about 38 s (A-level estimate from the B1 figures).
11. **What reruns it today.** Everything in B1 (same declared inputs), plus any change to the host pass's outputs.
    host.log is not byte-stable, so a rerun host pass always reruns this pass. It shares the same inputs anyway.
12. **What SHOULD rerun it.** Engine behaviour (as B1) and the host pass's ledger and rosters.
13. **Who runs it today.** As B1: build //..., lane 4 and the build lane, the bump, humans. Not the local gate.
14. **Recommendation.** **TEST-IN-DISGUISE.** Manual: yes, with the lane suite naming it. Writer `_4`: deliberate
    human re-bless only. Diff test: lane 4 and the bump's check. Narrowing: A9b. The unused `judge-database.tsv`
    should either be uploaded or dropped from the declared outputs (OPEN: whether anyone reads it by hand).
15. **Open questions.** As B1; plus whether `judge-database.tsv` has a human reader.

### B3. `//spec:judge_host_h2`

1. **Identity.** `//spec:judge_host_h2`; `_java_run` (testonly), from `corpus_lane` at `spec/BUILD.bazel:196-203`
   (body corpus.bzl:72-90), with `-Drcorpus.backend=h2`. Program: JUnitAction over `MinimalCorpusTest#corpus`.
2. **What it computes.** The host-judge corpus run on a fresh in-memory H2 per session. The golden runs on the same
   session: a portability check, not an independent oracle (`MinimalCorpusTest.java:981-985`). It produces the
   ledger, the four H2 rosters and a verdict.
3. **Why it exists.** Maven gate 5 (docs/GATES.md:22). It was made an action in `c1307ee81` and `e8dcffb38`. The user
   decided on 2026-09-08 to keep H2 as a portability lane (`MinimalCorpusTest.java:981-984`). The reason still holds.
4. **Inputs, declared.** 17,180, the same groups and counts as B1 (aquery). 33 core jars.
5. **Inputs, actually read.** As B1, with the `h2-*` policy files. Over-declared as B1.
6. **Outputs.**
   - `judge-host.tsv`, `verdict.txt`, `host.log`.
   - `h2-fail-roster.txt`: 361 lines committed.
   - `h2-skipped-roster.txt`: 14.
   - `h2-unordered-register.txt`: 877.
   - `h2-engine-order-register.txt`: **empty by construction** (A7.4).
   - `judge_host_h2_tmp/`.
7. **Committed?** The 4 rosters, at `spec/src/test/resources/rcorpus/h2-*.txt`. Writers `update_rcorpus_h2_0..3`,
   umbrella `update_rcorpus_h2`. Diff tests `_0..3_test`, in `update_rcorpus_h2_tests`, in `corpus_h2`. Not in
   `//:generated` or `//:update_generated`.
8. **Who consumes it.** `judge_database_h2` (all 7), `corpus_h2_verdict`, the writers and diff tests `_0..3`, the CI
   upload, humans (GATES.md:22, 74-77).
9. **Determinism.** As B1. A historical note: before P2-15 the H2 fail roster carried messages with random
   `executionTraceID`s (`c1307ee81`). Rosters now hold names only (`writeRoster` strips ` :: reason`).
10. **Cost.**
    - `memory_mb = 4096`: "live peak 3235 MB, measured 2026-10-04", deliberately under the rule's 4864 for the 7 GB macOS
      runner (spec/BUILD.bazel:193-195).
    - One JVM with in-memory H2 sessions.
    - The prerun-era lane, both passes: 78.0-81.9 s (GATES.md:6085, 6443); heap 4,285 / live 3,913 MB then.
11. **What reruns it today.** As B1.
12. **What SHOULD rerun it.** Engine behaviour on H2 (core main, rcorpus/harness, the h2 policy files, the H2 driver)
    and the upstream bump.
13. **Who runs it today.** `bazel build //...`; CI lane 5 and the build lane on linux, macos (4 GB on the 7 GB
    runner) and windows; the bump; humans. Not the local gate.
14. **Recommendation.** **TEST-IN-DISGUISE.** Manual: yes, run through `//spec:corpus_h2`. Writers: deliberate per-lane
    re-bless. Diff tests: lane 5 and the bump's check. Narrowing: A9b. Consider dropping `h2-engine-order-register`
    from H2's measured set. It cannot hold a row while the scan-order flag is DuckDB-only; that is the user's call.
15. **Open questions.** As B1. Whether the empty H2 engine-order registers should keep a writer and a diff test:
    settle by confirming that StableScanOrder never applies to H2 (`TestLaneOrderGuardrailTest.java:78-87`).

### B4. `//spec:judge_database_h2`

1. **Identity.** `//spec:judge_database_h2`; `_java_run` (testonly), from `corpus_lane` at
   `spec/BUILD.bazel:196-203` (body corpus.bzl:91-115). Program: JUnitAction, database mode, `rcorpus.backend=h2`.
2. **What it computes.** The database-judge corpus run on H2, joined per assert to `judge_host_h2`'s ledger and held
   against its fail roster through `h2-database-{lost,gained}-register.txt` (65 and 72 rows). The untriaged work list
   (`h2-database-untriaged-register.txt`, 8 rows) must only shrink. It measures `h2-database-engine-order-register.txt`.
3. **Why it exists.** Gate 5's database half ("joined since 2026-09-23", GATES.md:22); an action since `e8dcffb38`.
4. **Inputs, declared.** 17,187 (aquery): the host's inputs + 7 host outputs. 33 core jars.
5. **Inputs, actually read.** As B2, with `h2-*` files. Over-declared as B1.
6. **Outputs.** `judge-database.tsv` (no consumer), `verdict.txt`, `database.log`,
   `h2-database-engine-order-register.txt` (empty by construction, A7.4), `judge_database_h2_tmp/`.
7. **Committed?** The register; writer `update_rcorpus_h2_4`; diff test `update_rcorpus_h2_4_test`, in
   `update_rcorpus_h2_tests`, in `corpus_h2`.
8. **Who consumes it.** `corpus_h2_verdict`, `update_rcorpus_h2_4(_test)`, the CI upload, humans. The ledger: nobody.
9. **Determinism.** As B1.
10. **Cost.** 4096 MB. It never runs at the same time as the host pass, since it waits for the host's outputs
    (spec/BUILD.bazel:194-195). The whole corpus once more.
11. **What reruns it today.** As B2.
12. **What SHOULD rerun it.** As B3, plus the host pass's ledger and rosters.
13. **Who runs it today.** As B3.
14. **Recommendation.** **TEST-IN-DISGUISE**, as B2. Manual yes; writer deliberate; diff test in lane 5 and the bump's
    check; narrowing A9b.
15. **Open questions.** As B2 and B3.

### B5. `//spec:judge_host_warehouse`

1. **Identity.** `//spec:judge_host_warehouse`; `_java_run` (testonly, **manual**), from `corpus_lane` at
   `spec/BUILD.bazel:169-190` (`golden = False`, `rosters = "duckdb"`, `tags = ["manual"]`). Program: JUnitAction over
   `MinimalCorpusTest#corpus`, host mode.
2. **What it computes.** The DuckDB corpus run with every connection a session on the warehouse server. The pass
   starts the native server as a child process and talks to it over its JDBC driver. It shows that the warehouse
   gives the same verdicts as in-process DuckDB: its fail, skipped, unordered and engine-order results are checked
   EXACTLY against DuckDB's committed rosters (A2; `MinimalCorpusTest.java:917-958, 1189-1199`). It produces a verdict,
   a log and a ledger.
3. **Why it exists.**
   - Warehouse W1c (`3d549bc1d`, 2026-09-26; docs/WAREHOUSE_W1_DESIGN_2026_09_26.md:204-215): "the DuckDB corpus lane
     gives the same verdicts through the warehouse".
   - An action since `c1307ee81`. The native binary has been used, not a child JVM, since P3-19.
   - `77f4107c1` restored its exact checks ("they had stopped checking anything").
   - The reason holds while the warehouse is meant to be a drop-in DuckDB.
4. **Inputs, declared.** 17,189 (aquery):
   - B1's groups;
   - `:corpus_srcs` 70 + 5 committed DuckDB rosters (corpus.bzl:63-66, 75);
   - `//warehouse:server_native` (`server_native-bin`) and `//warehouse:duckdb_library` (`libduckdb_java.so_osx_universal`
     on darwin) (spec/BUILD.bazel:178-181);
   - jars `warehouse/client` and `warehouse/sqlapi`. 33 core jars.
5. **Inputs, actually read.** As B1, plus:
   - the committed DuckDB rosters, through `rcorpus.committed`'s directory: fail, skipped, unordered and engine-order.
     The database-engine-order file is declared but read only by B6;
   - the server binary (executed) and the DuckDB library (its path passed as `--duckdb-library`).

   Over-declared as B1, and `duckdb-database-engine-order-register.txt` for this pass. Under-declared: none found.
   OPEN: the server's own reads (its data dir is under the action tmpdir).
6. **Outputs.** `judge-host.tsv`, `verdict.txt`, `host.log` (includes `[warehouse]` server stderr),
   `judge_host_warehouse_tmp/` (includes the warehouse's data directory). No rosters.
7. **Committed?** No (`golden = False`, corpus.bzl:62, 138). No writer, no diff test.
8. **Who consumes it.** `judge_database_warehouse` (verdict, log, ledger) and `corpus_warehouse_verdict` (verdict,
   log). Humans: docs/WAREHOUSE_W1_DESIGN_2026_09_26.md:208, GATES.md:6749, docs/SERVER_PROGRAM_2026_09_26.md:219. Not
   uploaded by CI (never built there).
9. **Determinism.** As B1 for the verdict and ledger. The server adds timing and port lines to the log. OPEN: whether
   server-side nondeterminism (concurrency 4) can move a verdict. W1c measured the roster identical (`3d549bc1d`).
10. **Cost.**
    - 1024 MB JVM heap (`resource_set` cpu 1, memory 1024).
    - **Plus the native warehouse server process,** whose memory and CPU (`--concurrency 4`) are not in the resource_set.
      OPEN: its RSS.
    - Building `//warehouse:server_native` is itself a heavy native-image compile.
    - About 52 s per pass against about 35 s in process (`3d549bc1d`).
11. **What reruns it today.** As B1, plus the 5 committed DuckDB rosters (so a duckdb re-bless reruns it), any
    `//warehouse` server, sqlapi or client source, the GraalVM pin and the C toolchain (through `server_native`;
    `somepath(//spec:judge_host_warehouse, //warehouse:server_native)` is direct).
12. **What SHOULD rerun it.** Engine behaviour, warehouse server and client behaviour, and the DuckDB rosters it
    compares to.
13. **Who runs it today.** Nobody automatically: manual, no CI lane (gates-run.yml:50-65), not `//gates:local`, not
    the bump (`bazel test //...` skips manual). By hand: `bazel test //spec:corpus_warehouse`. Planned runner: P5-08's
    weekly `//gates:heavy`, not yet built.
14. **Recommendation.** **TEST-IN-DISGUISE.** Manual: yes (as now). No writer (`golden = False`), so no update group.
    Its verdict belongs in a scheduled heavy lane (P5-08), or on demand when warehouse or engine changes land. Narrowing
    as A9b, and keep the 5 DuckDB rosters as its only spec-tree file inputs.
15. **Open questions.** The server's RSS and reads; whether concurrency can move verdicts; when P5-08 lands (the
    workplan item).

### B6. `//spec:judge_database_warehouse`

1. **Identity.** `//spec:judge_database_warehouse`; `_java_run` (testonly, **manual**), from `corpus_lane` at
   `spec/BUILD.bazel:169-190` (body corpus.bzl:91-115). Program: JUnitAction, database mode.
2. **What it computes.**
   - The database-judge corpus run through the warehouse, joined per assert to `judge_host_warehouse`'s ledger.
   - Its fail list is held against **DuckDB's committed** fail roster. There is no `rcorpus.host.measured` when
     `golden = False`, so `readRoster` reads the committed copy (`MinimalCorpusTest.java:1192-1196`).
   - Its engine-order register is checked exactly against the committed `duckdb-database-engine-order-register.txt`.
   - The DuckDB database policy registers apply.
3. **Why it exists.** As B5 (W1c's database half; "host 2,472 / 107 and database 2,474 / 107", `3d549bc1d`).
4. **Inputs, declared.** 17,192 = B5's 17,189 + 3 host outputs (verdict, log, ledger) (aquery). 33 core jars.
5. **Inputs, actually read.** As B2, except that the host rosters are replaced by the committed DuckDB copies, plus the
   server binary and library. Over-declared as B1.
6. **Outputs.** `judge-database.tsv` (no consumer), `verdict.txt`, `database.log`, `judge_database_warehouse_tmp/`.
7. **Committed?** No.
8. **Who consumes it.** `corpus_warehouse_verdict` (verdict and log) and humans (as B5).
9. **Determinism.** As B5.
10. **Cost.** As B5: 1024 MB plus a second warehouse server process, started by this pass. It runs after the host pass.
11. **What reruns it today.** As B5, plus the host warehouse outputs.
12. **What SHOULD rerun it.** As B5.
13. **Who runs it today.** As B5: by hand only.
14. **Recommendation.** **TEST-IN-DISGUISE**, as B5. Manual yes; no update group; scheduled heavy lane; narrowing A9b.
15. **Open questions.** As B5; and whether `judge-database.tsv` has a reader.

---

## Part C. Table

| Label | Recommendation | Manual? | Update group | True trigger |
|---|---|---|---|---|
| `//spec:judge_host_duckdb` | TEST-IN-DISGUISE | yes (now no) | its writers `update_rcorpus_duckdb_0..3`: deliberate per-lane re-bless; neither `//:update_generated` nor bump-only | engine behaviour on DuckDB: core main on the corpus path, rcorpus/harness code, the duckdb policy files, the DuckDB driver; an upstream bump |
| `//spec:judge_database_duckdb` | TEST-IN-DISGUISE | yes (now no) | `update_rcorpus_duckdb_4`: deliberate re-bless | as above + the host pass's ledger and rosters |
| `//spec:judge_host_h2` | TEST-IN-DISGUISE | yes (now no) | `update_rcorpus_h2_0..3`: deliberate re-bless (`_3` holds an empty file by construction) | engine behaviour on H2: core main, rcorpus/harness, the h2 policy files, the H2 driver; an upstream bump |
| `//spec:judge_database_h2` | TEST-IN-DISGUISE | yes (now no) | `update_rcorpus_h2_4`: deliberate re-bless (empty by construction) | as above + the host pass's ledger and rosters |
| `//spec:judge_host_warehouse` | TEST-IN-DISGUISE | yes (already) | none (`golden = False`) | engine behaviour, warehouse server and client changes, DuckDB's committed rosters |
| `//spec:judge_database_warehouse` | TEST-IN-DISGUISE | yes (already) | none | as above + its host pass's outputs |
