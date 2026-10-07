# Bazel audit: core/src/test, core/src/main/duckdb, core/BUILD.bazel

Audited at origin/main 16c8120d5 (2026-10-02)

**Slice:** `core/src/test` (Java tests and their resources; the `.pure` stress corpus files were checked by header only), `core/src/main/duckdb`, and `core/BUILD.bazel` for context.

I found 19 new findings: 1 P0, 7 P1 and 11 P2. Six known items are re-confirmed with line evidence. Nothing in the tree was modified. The only files I wrote are scratch dumps under `runs/` (gitignored). All paths are repo-relative, and `T/` means `core/src/test/java/com/legend/`.

Severity: **P0** breaks hermeticity or correctness, **P1** is a non-Bazel workflow, **P2** is hygiene.

## Section 1 — NEW findings

### N1 (P0). Fixed-range port in a gate-1 test

- Evidence: `T/server/PureLspServerTest.java:197-198`, `int port = 19876 + new Random().nextInt(100); new LegendHttpServer(port)`.
- What it does: it binds one of 100 fixed ports. It collides with parallel test runs, a second checkout or anything else on the host, and it is non-deterministic.
- Every other server test uses port 0: `DiagramServiceTest:262,301`, `PureV1HttpTest:47` and `LegendHttpServerIntegrationTest:42`.
- Fix: `new LegendHttpServer(0)` plus `getPort()`. Add a guardrail that bans literal ports in tests.

### N2 (P1). CorpusDifferentialTest can never run under Bazel; it skips silently in gate 1

- `T/integration/CorpusDifferentialTest.java:38` reads `DIFF = Repo.out("diff")`, which is `$TEST_UNDECLARED_OUTPUTS_DIR/diff`, always empty when a test starts.
- Line 44 is `Assumptions.assumeTrue(Files.isDirectory(DIFF.resolve("expected")), "run scripts/corpus/differential.py first")`.
- The generator writes elsewhere: `scripts/corpus/differential.py:8-9,36` writes `core/target/diff`.
- The javadoc at line 20 points at `tools/allgates.sh`, which does not exist.
- The skip is registered as legitimate in `T/SkipCensusTest.java:57-59`.
- It also formats with `String.format("%.6f")` (line 146), which depends on locale.
- Result: a permanently skipped test inside `//core:core_tests`.
- Fix: a Bazel action runs the generator (declared Python toolchain, oracle inputs) and produces `seed.sql` and `expected/`. The differential becomes its own `junit_test` whose data is that output, located through the runfiles library. Otherwise delete the test and its SkipCensus row.

### N3 (P1). StressServiceSuitesTest hides a second lane and hand-run modes behind -D flags that no target sets

- Evidence in `T/integration/StressServiceSuitesTest.java`:
  - Line 36, `stress.data`: an override path resolved relative to the working directory, i.e. outside declared data.
  - Lines 40-44 and 101, `stress.rows`: writes one JSON file per test to an arbitrary host directory.
  - Line 63, `stress.only`.
  - Line 65, `stress.backend=h2`.
  - Line 79, `stress.sessions`.
  - Lines 162-167: with `stress.data` set, the test returns before any assertion and passes vacuously.
- The H2 ratchet `MIN_PASS_H2 = 4622` (line 183) is enforced by no target. `//core:stress_suites` (`core/BUILD.bazel:397`) sets no `jvm_flags`.
- The comments at lines 94 and 175 still say `tail -f core/target/...`.
- Fix:
  - Add a `stress_suites_h2` `junit_test` with `jvm_flags = ["-Dstress.backend=h2"]`.
  - Move the rows and override modes into a `bazel run` `java_binary` that writes to `$BUILD_WORKSPACE_DIRECTORY` or the outputs dir.
  - Delete the knobs from the test.

### N4 (P1). The test JVM's locale is not pinned, so the host locale gets in (all lanes)

- `tools/junit/defs.bzl` only adds `-Duser.timezone=GMT`.
- Default-locale formatting in product code: `PureDateLiteral.java:97-350` and `PureTimeLiteral.java:54-77` use `String.format("%d-%02d…")`. `RelationalDataType.java:123`, `JoinType.java:30`, `Executor.java:1064`, `AnsiSqlRenderer.java:1379` and `CorrelatedSubselects.java:2064,2073` call `toUpperCase()`/`toLowerCase()` with no Locale.
- Tests that depend on it:
  - Date rendering in `TypeInferenceIntegrationTest:491,501,512,1533,3042-3062`, `AuditRound3Test:192`, `CanonicalFormTest:81-95` and `PureAssertsTest:169-174`.
  - Locale-less case folding in `SpecParserTest:249,293,344,1282`, `ResolveSimpleClassTest:136`, `GetCheckerTest:448`, `VariantIntegrationTest:397`, `ModelIndexerTest:503-536`, `UserFunctionIntegrationTest:531,551,678`, `ElementParserTest` (e.g. 508,1203,2613), `RelationalMappingCompositionTest:91,101,106`, `RelationApiIntegrationTest:138`, `WriteCheckerTest:122,134`, `UnionTargetLeanJoinTest:127`, `TypeConversionCheckerTest:372`, and many in `RelationalMappingIntegrationTest` (1937-5100).
  - `CorpusDifferentialTest:146` (`%.6f`).
- Fix:
  - Add `-Duser.language=en -Duser.country=US` (or `-Duser.language=` with `Locale.ROOT`) to the `junit_test` macro.
  - Change product sites to `Locale.ROOT`.
  - Add a guardrail against locale-less `format`/`toUpperCase`/`toLowerCase` in `core/src/main`.

### N5 (P1). Gate 1 depends on test order, through process-wide static state in its one big JVM

- `T/server/LegendHttpServerIntegrationTest.java:26` uses `@TestMethodOrder(OrderAnnotation)`. `@Order(7) testEngineExecutePureQuery` (line 338: "Data was inserted in Order 4") only passes after `@Order(3) seedPersonTable`. Running one method with `--test_filter` fails.
- `ConnectionResolver.STORE` (`core/src/main/java/com/legend/server/ConnectionResolver.java:34`) is a static `HandleStore` keyed by store text. `T/server/Seed.java:20-25` writes into those cached in-memory DBs, and `ConnectionIsolationTest:67-68` runs `CREATE TABLE LEAK_T` (no `IF NOT EXISTS`). The table outlives the class for the rest of the JVM. A second run in the same JVM, or any class that seeds identical store text, collides. Classes that share the cache: `ConnectionIsolationTest`, `ConnectionLeaseTest`, `LegendHttpServerIntegrationTest` and `StreamingIntegrationTest` (through `QueryService`).
- These are deterministic today only because `core_tests` runs every class in one JVM, sequentially.
- Fix:
  - Make each test independent: seed inside the test, use unique store names, or add a test-scoped reset hook on `ConnectionResolver`.
  - Remove `@TestMethodOrder`.
  - Longer term, split gate 1 into per-package `junit_test` targets so a leak cannot cross classes.

### N6 (P1). A test that launches a child JVM itself; the planner check belongs in the build graph

- `T/PlannerRunsOnJavaBaseTest.java:30-35` runs a `ProcessBuilder` on `java.home/bin/java --limit-modules java.base -cp ${java.class.path} PlanOnJavaBase`.
- `T/PlanOnJavaBase.java:13` is a `main()` program sitting in test sources.
- This relies on `java.class.path` being a plain path list, which breaks under the classpath-jar or manifest launchers Bazel uses for long classpaths and on Windows.
- Fix: a `java_test` (or `java_binary` plus `sh`-free test) with `main_class = "com.legend.PlanOnJavaBase"` and `jvm_flags = ["--limit-modules=java.base"]` that asserts its own output. Better, the planned planner target compiled with `--limit-modules`.

### N7 (P1, adjacent to my slice). tools/census/render.sh uses the core test jar as a library from bash, and works only on Apple-silicon macOS

- `tools/census/render.sh:9` hard-codes `external/rules_java++toolchains+remotejdk25_macos_aarch64/bin`.
- Line 12 runs `bazel build //core:core_tests_deploy.jar`.
- It then runs `javac` and `java` by hand on `tools/census/RenderCensus.java`.
- Fix: a `java_binary` `//tools/census:render` that depends on `//core` and is run with `bazel run`, or `target_compatible_with` if it must stay platform-specific.

### N8 (P2). Tests write temp files to the host temp directory, not TEST_TMPDIR

- The macro never sets `-Djava.io.tmpdir=$TEST_TMPDIR`, so these writes land in the host `/tmp`:
  - `ConnectionLeaseTest:65,235` (`Files.createTempFile`)
  - `LegendHttpServerIntegrationTest:38`
  - `JsonM2MChainIntegrationTest:954-959` (createTempDirectory plus deleteOnExit)
  - `SavedQueriesTest:20` (`@TempDir`)
- With hermetic `/tmp` sandboxing on, these behave differently from local runs.
- Fix: set `java.io.tmpdir` from `TEST_TMPDIR` in JUnitMain or the macro.

### N9 (P2). NoEagerTypeReferencesTest finds classes by listing Bazel output jars

- `T/architecture/NoEagerTypeReferencesTest.java:71-86` lists the siblings of `TypedClass`'s code-source jar, matching `lib[a-z_]+\.jar` and excluding `tests_lib` and `libcore_next`.
- That ties it to `rules_java` jar naming and to the runfiles directory layout.
- Line 136 silently skips any class that fails to load (`catch (Throwable) { return; }`).
- Fix: pass the 29 core jars explicitly, either with `$(rootpaths)` or the existing `Repo.listed`/`java_jars` mechanism. Fail on unloadable classes.

### N10 (P2). `core/src/main/duckdb` is invisible to most guardrails, and its bulk-load path has no dedicated test

- Every guardrail walks `Repo.module("src/main/java")`: CodeShape, ErrorShape, Observability, TenetRatchet, PlatformNames, Identity and the rest. Only `JdbcSurfaceCensusTest` (ROOTS `"core/src"`, lines 74-75) sees `DuckDbAppenderLoad.java`.
- ArchitectureTest's classpath in `//core:guardrails` (`core/BUILD.bazel:350`) has no `:duckdb_load`, so the ArchUnit rules never import it.
- `BulkLoad` is found through a ServiceLoader, and with no provider it falls back to the text path silently (`BulkLoad.java:7-9`). Whether the Appender path is exercised therefore depends on whether a target happens to include `:drivers`.
- `RowLoadTest:71-168` asserts `Census.BULK_LOADS` deltas, so it needs `:duckdb_load`. `core_tests_lib` does not declare that; only `core_tests` brings it in through `:drivers`.
- Fix:
  - A small `junit_test` for `:duckdb_load` that asserts the provider is found and that its rows are identical to the text path.
  - Add `src/main/duckdb` to the guardrail roots.
  - Declare the `:duckdb_load` dependency on the test library.

### N11 (P2). Timing- and clock-sensitive assertions

- Wall-clock budgets:
  - `T/resolver/StackShapeWitnessTest.java:219`: `assertTrue(ms < 5_000)`.
  - `T/integration/StreamingIntegrationTest.java:391-424,543-573`: a 1 ms sampling thread with a busy-wait, asserting at least 3 intermediate sizes. Flaky on loaded CI.
  - `cache/HandleStoreTest:72-104`: `await()` and `join()` with no timeout, so a regression hangs until the 3600 s Bazel limit.
- Clock:
  - `integration/ExtendCheckerTest:1968-1996` (`testToday`/`testNow`) compares DuckDB `today()`/`now()` with JVM `Year.now()`. DuckDB uses the host timezone, the JVM uses GMT, so it fails around New Year.
  - Only the JVM timezone is pinned. Raw-JDBC tests skip DuckDb's `SET TimeZone='UTC'` session prelude: `ComputedProjectIntegrationTest:86-408`, `TypeInferenceIntegrationTest:39-42` and `ExecutionResultIntegrationTest:33-35` each set UTC by hand, while `AuditRound3Test`, `DuckDbValidityTest:213-220` and `ResolveNestedTemporalFrameTest` do not.
- Fix: remove the budgets or move them to a benchmark target; use timed waits; inject the clock; use one shared connection factory that pins the timezone.

### N12 (P2). Dead and vacuous tests in gate 1

- `integration/TypeInferenceIntegrationTest:1920-1928`: `testContainsPrimitive()` has no `@Test` and never runs.
- `RelationalMappingIntegrationTest`: 15 `@Disabled` tests with empty bodies (lines 2386, 2437, 2445, 3045, 3108, 3337, 5219-5256, 5480, 5486). Two vacuous green tests, `testXStore:3321` and `testAggregationAware:3329`, have comment-only bodies.
- Tests that assert nothing:
  - `ResolveUnionV4ProbeTest:109-114,162-167` and `ResolveUnionSelfJoinProbeTest:69-74` only print SQL.
  - `VariantIntegrationTest.testRawSqlUnnest:548-619` ends with `assertTrue(true)`.
  - `DuckDBStructSyntaxTest:25`.
  - `M2MIntegrationTest:762-763,782` has its checks commented out.
  - `ResolveM2mTest:204-206` asserts `… || msg != null`, which is always true.
- Fix: delete them, convert them to `@KnownDefect`, or move them to `bazel run` tools. Add a guardrail that flags any `void test*` without a JUnit annotation.

### N13 (P2). A gate-1 test reaches stress-lane data that gate 1 does not declare

- `T/integration/LegendLiteGapTest.java:141-151` reads `StressCorpus.EXCLUDED`. That runs StressCorpus's static initialiser, which resolves `Repo.module("src/test/resources/stress")` and `Repo.path("projects")` (`T/integration/StressCorpus.java:35-36`).
- `projects` is not in `core_tests` data (`_CORE_READS`, `core/BUILD.bazel:266`). It only works because nothing is read.
- Fix: move EXCLUDED to a holder class with no Repo static initialiser.

### N14 (P2). Shared mutable statics inside the one JVM (latent; they break if JUnit parallelism is turned on)

- `CanonicalDivergence` keeps static counters (`exec/CanonicalDivergence.java:37-38,220-289,534`).
  - `V7DualChannelCensusTest:21-80` and `CanonicalFormTest:119,138` call `reset()` and assert exact global counts.
  - `LiteralChannelTest:63-102`, `AssertVerdictsTest:128-144` and `InstanceIdentityTest:156-258` assert before/after deltas.
- `RowLoadTest` asserts `Census` deltas.
- The `ToySectionGrammar` ServiceLoader registration (`core/src/test/resources/META-INF/services/com.legend.spi.SectionGrammar`) ships in `core_tests_lib` resources (`core/BUILD.bazel:282`). Every gate-1 test therefore parses with an extra "Toy" grammar. `ToySectionGrammar.java:12` keeps a `static volatile lastText` that `SectionGrammarRegistryTest:85` reads.
- Class-shared DuckDB connections that test methods add tables to: `LowerRelationTest:48-75` (T_ORDERS, T_DOCS…), `ExecuteInDbTest:37-46`, and `ResolveNestedNavTest:160-172` (inserts, then deletes in a `finally`).
- Fix:
  - Inject the counters, or `@ResourceLock`.
  - Give the toy binding its own testonly library on a dedicated target (as already done with `:shadow_binding`).
  - Open one connection per method.

### N15 (P2). Row-order assertions on queries with no ORDER BY

- `DuckDBIntegrationTest:2758-2762, 3130-3135, 3232-3236, 3482, 3532, 3658, 3707, 6138-6282`.
- `ValueMapPlacementTest:230`, `JsonM2MIntegrationTest:150`, `FromCheckerTest:208`, `ExtendCheckerTest` (156, 221, 435, 1223, 1528).
- `RelationalMappingIntegrationTest:2575, 2624`, `ResolveGraphUnionProbeTest:363, 411`, `ConcatenateFlattenCheckerTest:356`.
- `ResolveSimpleClassTest:150-327`, `LowerRelationTest:179, 357`, `LetCheckerTest:218, 263`, `GraphFetchCheckedIntegrationTest:305`.
- Fix: add a sort, or compare as a multiset.

### N16 (P2). Guards silently skip roots that are missing

- `LegacyReachbackCensusTest:117`, `SkipCensusTest:180`, `JdbcSurfaceCensusTest:169` and `ParserBoundaryArchTest:166-171` all `continue` past a missing root. `ParserBoundaryArchTest` still lists `"server/src"`, a module that no longer exists.
- Only file-count floors back these up. `VerdictChannelRegisterTest:67` and `DropInSurfaceTextRuleTest:163` show the right pattern: fail.
- Fix: fail on a missing root, and derive the roots from declared data.

### N17 (P2). Hand-typed pins that a generator could produce

- `builtin/EngineHandlersTest:59-62` pins 404/836/168, all derivable from the already generated and diff-tested `engine-handlers.tsv`.
- `builtin/NativeFunctionTest:634,1557` pins counts and cites `tools/m3shape.py` and `tools/shape_sweep.py`, neither of which exists.
- `MIN_PASS` and `MIN_PASS_H2` (`StressServiceSuitesTest:183-184`).
- The roughly 25 guardrail and census ratchet constants are hand-edited by design under AGENTS.md. They are only pins, but they live in JUnit rather than build rules.
- Fix: have generators emit the counts into goldens covered by `write_source_files`.

### N18 (P2). gate 1 declares data it does not read; the stress corpus ships twice

- `core_tests` declares `data = glob(["src/**"])` (`core/BUILD.bazel:266,318`). Yet only 4 non-tagged tests read by path: LeanSqlLadderTest, CorpusDifferentialTest, StressServiceSuitesTest (another lane) and StressCorpus via N13.
- The 13 MB stress corpus is both a jar resource (`core/BUILD.bazel:282`) and runfiles data, but it is read by path.
- The ladder pins are on the classpath too, yet are read by path.
- `_CENSUS_READS` adds `//projects:srcs`, which no census test reads.
- Fix: read the ladder pins as classpath resources, drop `data` from `core_tests`, and give each lane exactly the files it reads.

### N19 (P2). Path-walking guards need a runfiles directory tree

- 29 files walk directories through `Repo` (`Files.walk`/`Files.list`), so they need a real runfiles tree.
- This is why `.bazelrc` forces `build:windows --enable_runfiles` and `--windows_enable_symlinks`.
- The files: ParserBoundaryArchTest, CarrierPurityRatchetTest, CodeShapeGuardrailTest, DanglingStateGuardTest, DialectBoundaryTest, ErrorShapeGuardrailTest, FallbackLedgerTest, HarnessDisciplineTest, IdentityGuardrailTest, CorpusDifferentialTest, StressCorpus, StressServiceSuitesTest, JavaEvalLedgerTest, JdbcSurfaceCensusTest, LeanSqlLadderTest, LegacyReachbackCensusTest, LiteralUnrollLedgerTest, ObservabilityGuardrailTest, ParkedWorkLedgerTest, DropInSurfaceTextRuleTest, PlatformSurfaceGuardrailTest, PlatformNamesGuardrailTest, RawSqlLedgerTest, ShadowWalkerCensusTest, SkipCensusTest, SqlTextRatchetTest, TenetRatchetTest, TestLaneOrderGuardrailTest, VerdictChannelRegisterTest.
- Fix: pass a declared file list (`$(rootpaths)`, resolved through the runfiles library) instead of walking directories.

## Section 2 — KNOWN items re-confirmed

### K1

- 14 of the 202 files in `core/src/test/resources/stress` (13 MB) carry generator markers.
- Header markers: `build.py` (3), `combos.py` (2), `hier.py`, `dense_store.py`, `dense_mapping.py`, `oracle.py`, `battery.py`.
- References inside files: `deepstack.py` 2463, `taxonomy.py` 1947 and others.
- There is no generating action and no diff test.
- `StressCorpus.java:25-28` keeps `LINKED_PROJECTS` "in sync with scripts/corpus/model.py" by hand.

### K5

- `T/ladder/LeanSqlLadderTest.java:140-141` sets `PINS = Repo.module("src/test/resources/ladder")` and `RECORD = System.getProperty("ladder.record")`.
- Line 151 runs `Files.createDirectories(PINS)` on every run.
- Lines 253-255 run `Files.writeString(currentPin…)`. This writes through the runfiles symlink into the source tree, and only when unsandboxed.
- The javadoc at line 49 refers to `ladder/register.txt`, which does not exist.
- The 12 `*.current.sql` files are generator output outside `write_source_files`.

### K12

- `testing/src/main/java/com/legend/testing/Repo.java:44-77` is a hand-rolled TEST_SRCDIR/TEST_WORKSPACE/TEST_TARGET resolver; the module is derived from the `TEST_TARGET` label.
- `:148-157` puts generator side reports in `Files.createTempDirectory`.
- `Repo.out` is used by IdentityGuardrailTest:183, StressServiceSuitesTest:92-146, CorpusDifferentialTest:38 and LeanSqlLadderTest:260.

### K15

- `core/BUILD.bazel:266-275`: `_CORE_READS = glob(["src/**"])`. See N18: gate 1 barely needs it.
- `core_tests` (`:318-334`) is a single "enormous" `java_test` with `select-package` plus `exclude-tag`s.
- The `"heavy"` exclusion is redundant: the two heavy classes, `ProfileBuildCost:13` and `StressTestChaotic:36`, never match JUnit's default class-name pattern anyway.

### K18

- `scale_*` (`core/BUILD.bazel:378-392`) is `manual` and in no CI lane. It covers StressTest10K/100K/Dense/ComplexQueries/Chaotic and ProfileBuildCost, which print timings and assert only "no failures".
- The BUILD comment says "50K chaotic"; the test is 100K (`StressTestChaotic:100-103`).
- Gate-1 `exclude_tags` and the stress tag (`StressServiceSuitesTest:25`) do work.
- No test class is selected by more than one lane. The only classes not selected by the default pattern are the six `_SCALE` classes, and they are selected explicitly.

### Guardrails and censuses as JUnit (re-confirmed)

- 18 `@Tag("guardrail")` and 9 `@Tag("census")` classes are static analysis over source text: regex walks with hand floors in `GuardCoverage.java:27`.
- ArchitectureTest is ArchUnit with a code-source-path workaround (`ArchitectureTest.java:36-104`).
- `ObservabilityGuardrailTest:58` allowlists `TEST_UNDECLARED_OUTPUTS_DIR`, because the product class `probe/Shadow.java:88-99` reads it.
- `IdentityGuardrailTest:183` writes `identity-sites.tsv` to undeclared outputs, which is correct.

## Section 3 — Coverage

**Read in full by me (60 files that matched the risk patterns, 16,379 lines):**

- Guardrail and census classes: GuardCoverage, ShadowWalker, PlatformSurface, TenetRatchet, LiteralUnroll, VerdictChannel, DialectBoundary, ParkedWork, TestLaneOrder, FallbackLedger, Observability, RawSqlLedger, LegacyReachback, NoEagerTypeReferences, SqlTextRatchet, DropInSurfaceTextRule, SkipCensus, ParserBoundaryArch, PlatformNames, JdbcSurfaceCensus, DanglingStateGuard, IdentityGuardrail, CarrierPurity, HarnessDiscipline, ErrorShape, CodeShape.
- Behaviour tests:
  - Planner and SQL: PlannerRunsOnJavaBase (and PlanOnJavaBase), H2FirstInGroup, H2SplitPart, Sha256, FrameQuotedColumn, WireTypes, ExecuteInDbProbeCount, QuotedColumnName, SqlCanonConformance, StackShapeWitness, LeanSqlLadder.
  - Integration: ProtocolReader, BazelSmoke, CorpusDifferential, StressCorpus, StressServiceSuites, StressTest, ProfileBuildCost, Streaming.
  - Server: PureV1Http, SavedQueries, ConnectionLease, DiagramService, PureLspServer, LegendHttpServerIntegration, PureV1Api.
- Read with only partial attention:
  - The StringBuilder model-generation loops in StressTest10K, 100K, Dense, ComplexQueries and Chaotic. I read setup, query building and assertions verbatim.
  - The ArchUnit rule bodies in ArchitectureTest. I read the head (classpath handling) in full.
  - The register literals in JavaEvalLedgerTest. I read its head and test methods (1290-1440).
  - The interior of JsonM2MChainIntegrationTest. Fully read: the resource/temp helper (930-975) and the connection setup.

**Read in full by four parallel sub-agents (the other 257 files, 93,559 lines):** they reported per-file results and I merged them above.

- One shortcut: the second half of `RelationalMappingCompositionTest` (729-1534). There the reader saw setup and structure, but its view filtered out the `assertEquals` lines.

**Also read:**

- `core/BUILD.bazel` in full.
- `core/src/main/duckdb`: `DuckDbAppenderLoad.java` and the service file.
- `testing/.../Repo.java`, `tools/junit/defs.bzl`, `.bazelrc`, `server/Seed.java`, and the relevant parts of `ConnectionResolver.java`.

**Skimmed only:** the headers of all 202 stress `.pure` files, scanned with grep for generator markers, plus `01-products.pure`. The `ladder`, `upstream-api`, `test-data` and `bazel_smoke` resources were listed, not read in full.
