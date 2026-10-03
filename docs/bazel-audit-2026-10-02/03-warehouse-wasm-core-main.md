# Bazel audit 03: warehouse, wasm, TeaVM, json, base, core main

Audited at origin/main 16c8120d5 (2026-10-02)

**Slice:** `warehouse/`, `wasm/`, `tools/teavm/`, `json/`, `base/`, `core/src/main`, and `core/BUILD.bazel`.

Nothing in the tree was modified during the audit. Scratch files were written only to a session scratchpad outside the repository.

## Section 1: NEW findings

Severity: **P0** = hermeticity or correctness, **P1** = non-Bazel workflow, **P2** = hygiene.

### N1 (P1). The native-image reachability metadata was recorded by hand, and nothing in Bazel can regenerate or check it

- **Evidence:**
  - `warehouse/src/main/resources/META-INF/native-image/com.legend/warehouse/reachability-metadata.json` (433 lines) is committed.
  - `warehouse/BUILD.bazel:57-61` says it was "Recorded by GraalVM's agent under the warehouse test suite" and globs it into `:server_lib`.
  - `docs/WAREHOUSE_W1_DESIGN_2026_09_26.md:333-338`: "Recorded by GraalVM's agent while the whole suite runs against the JVM server (one agent directory per process, merged)… **Owed:** re-recording as a Bazel target… the script that did it is gone."
- **The file records the recording host:**
  - Locale-specific resource bundles: `sun.text.resources.FormatData_en_US`, `cldr…TimeZoneNames_en_US` (json:87-116).
  - A JDK-build-specific resource glob, `jdk/internal/icu/impl/data/icudt76b/nfc.nrm` (json:131-133).
- **Why it matters:** the 39 FFM downcall shapes and 3 upcalls (json:136-432) mirror the descriptors in `server/duck/Duck.java:120-241` and `AuthenticatedUser.java:58-67`. `--link-at-build-time` does not check FFM descriptors. A new C function with a new shape breaks the binary at run time, and only `:tests_native` would notice. Nothing diff-tests the file.
- **Fix:** add a Bazel action that runs the warehouse suite with `-agentlib:native-image-agent=config-output-dir=…` from the `@graalvm` toolchain, plus a merge step. Register the output in `write_source_files` (`//:update_generated`) so a diff test guards it. Better still, generate the `foreign` section from the descriptor table in `Duck.java` with a small `java_run` generator, also diff-tested. Run the recording with the locale pinned.

### N2 (P0). On the JVM, the server and every JVM-mode warehouse test unpack DuckDB's native library into a shared, host-wide temp directory that persists between runs

- **Evidence:**
  - `warehouse/src/main/java/com/legend/warehouse/server/duck/DuckLibrary.java:77-102` (`extracted()`) writes to `Path.of(System.getProperty("java.io.tmpdir"), "legend-warehouse-duckdb")`.
  - The cache key is `name + "-" + size` (line 93). It reuses any existing file of the same size (line 94).
- **Callers:**
  - `WarehouseServer.java:146` via `Config.duckdbLibrary == null`.
  - `TestServer.java:68` always passes null in-process.
  - `PostgresCatalogTest.java:62` calls `DuckLibrary.load(null)`.
  - `AppModeTest` builds `WarehouseServer` directly (lines 189-193).
  - `//warehouse:server` (`BUILD.bazel:65-69`) does the same.
- **Effect:**
  - Tests write outside the test's temp directory, and the state is shared across runs, worktrees and branches.
  - A different DuckDB build of identical size is loaded silently, because the key is size, not content.
- **Why it is avoidable:** Bazel already extracts the exact library hermetically (`jar_entry` `:duckdb_library`, `BUILD.bazel:165-174`, `defs.bzl:11-24`).
- **Fix:**
  - Add `data=[":duckdb_library"]` to `:tests`, and an `env` entry with its `$(rlocationpath)`.
  - Have `TestServer` resolve it with the Java runfiles library and pass it as `Config.duckdbLibrary` in-process.
  - Give `//warehouse:server` the same data and an `--duckdb-library` argument.
  - Delete classpath extraction, or key it by content hash under `TEST_TMPDIR`.

### N3 (P1). Warehouse tests create temp directories outside `TEST_TMPDIR` and never delete them, and `junit_test` does not redirect `java.io.tmpdir`

- **The macro:** `tools/junit/defs.bzl:42` sets only `-Duser.timezone=GMT`. On Linux the JVM ignores `TMPDIR` and uses `/tmp`. On macOS it uses `/var/folders/…`.
- **Leaking call sites:**
  - `WarehouseArrowTest.java:44,100`
  - `WarehouseCorsTest.java:34`
  - `WarehouseJdbcTest.java:53`
  - `WarehouseServerTest.java:53,323,351`
  - `WarehouseEntitlementsTest.java:45,279`
  - `WarehousePostgresLiveTest.java:52`
  - `PostgresCatalogTest.java:63`
  - `AppModeTest.java:82-92`: `commandLine(--single-user)` without `--data` runs `Files.createTempDirectory("datacube-")` (`WarehouseServer.java:900-902`). The directory is removed only by the shutdown hook in `start()`, which the test never calls, so it leaks every run.
- **Effect:** each run leaves DuckDB databases, spilled results and Arrow files on the host.
- **Fix:**
  - In `JUnitMain` or the macro, set `java.io.tmpdir` to `$TEST_TMPDIR` (for example jvm_flags `-Djava.io.tmpdir=$${TEST_TMPDIR}`, which the java stub expands).
  - Use JUnit `@TempDir` everywhere.
  - Make `CommandLine.temporaryData` cleanup testable.

### N4 (P1). The shipped warehouse has no packaging rule, and the server's "beside the executable" mode exists only for a hand-assembled layout

- **Evidence:**
  - `DuckLibrary.java:37-59` (`besideExecutable`, `executableDir`) and `WarehouseServer.java:878-884` look for the library and the postgres extension next to the native binary.
  - No Bazel rule produces that layout. `MODULE.bazel` has no `rules_pkg`, and no `pkg_tar`/`pkg_zip` exists anywhere.
  - The only producer of a working layout is the bash launcher (`warehouse/defs.bzl:69-81`, K8), which passes explicit flags.
  - No test covers the beside-executable path: `TestServer.java:97-101` always passes `--duckdb-library`.
- **Fix:**
  - Add a `//warehouse:dist` (`rules_pkg` `pkg_tar`/`pkg_zip`) target per platform containing `server_native`, `duckdb_library` renamed, and the gunzipped `postgres_scanner.duckdb_extension`. `//datacube:app`'s site could be a variant of it.
  - Add a test that extracts the archive and runs the binary with no flags.

### N5 (P1). `//core:drivers` has no Postgres JDBC driver, but the shipped core server opens `jdbc:postgresql://`

- **Evidence:**
  - `core/src/main/java/com/legend/server/ConnectionResolver.java:215-224` calls `DriverManager.getConnection("jdbc:postgresql://"…)`.
  - `core/BUILD.bazel:225-233`: `:drivers` holds only h2, duckdb and sqlite.
  - `MODULE.bazel:73-83`: `@maven_core` has no `org.postgresql`.
  - Postgres drivers exist only in `maven_upstream` and `maven_runner`.
- **Effect:** `//core:server` and `server_deploy.jar` fail with "No suitable driver" on any Postgres connection. No test exercises this arm with the product's driver set.
- **Fix:**
  - Add `org.postgresql:postgresql` to `@maven_core` and `:drivers`.
  - Add a test on `:drivers` that drives the Postgres arm against `@embedded_postgres` (`testing/EmbeddedPostgres.java`).
  - Or, if this is intentional, make the arm refuse explicitly.

### N6 (P2). `//warehouse:tests_native` re-runs in-process unit tests and reports them as judging the binary, and the live Postgres test sits silently inside both suites

- **Evidence:**
  - `BUILD.bazel:189` selects `--select-package=com.legend.warehouse`. That includes `com.legend.warehouse.server.{AppModeTest, PostgresCatalogTest, IdentityTest}`, which build `WarehouseServer`/`Catalogs` in the test JVM (`AppModeTest.java:189-193`, `PostgresCatalogTest.java:62`).
  - `WarehousePostgresLiveTest` is in the same package. In `:tests` and `:tests_native` it is skipped by `@EnabledIfEnvironmentVariable` (line 39) instead of being excluded by the build.
- **Fix:**
  - Split `tests_lib` into an HTTP suite (`TestServer`-based) and unit tests; `tests_native` selects only the HTTP suite.
  - Exclude the live test by `--exclude-classname` or tag, so a skip can never mask it.

### N7 (P2). Tests and the server depend on the host account name

- **Evidence:** `WarehouseServer.java:888` takes `System.getProperty("user.name")`; `AppModeTest.java:88,179,185,193` assert on it.
- **Effect:** the tests fail on a host whose user name has a character outside `[A-Za-z0-9_.@-]` (a space, `DOMAIN\user`), and outcomes vary by runner account.
- **Fix:** inject the user (a `--user-name` flag or a supplier), use a fixed name in tests, or pin `-Duser.name=tester` in the target's jvm_flags.

### N8 (P2). Locale is not pinned for tests or build actions, and core output depends on the default locale

- **Evidence:**
  - Tests pin only `TZ` (`.bazelrc` `test --test_env=TZ=GMT`) and `-Duser.timezone=GMT`.
  - `java_run` actions (`wasm/BUILD.bazel:76-91` `jvm_answers`, and the spec generators) run under the host's locale, which Bazel's cache key does not cover.
- **Locale-sensitive calls in core main:**
  - `toUpperCase()`/`toLowerCase()` without `Locale.ROOT`:
    - `model/JoinType.java:30`
    - `model/RelationalDataType.java:123`
    - `normalizer/RelOpTranslator.java:308`
    - `exec/Executor.java:1064`
    - `exec/PctProbe.java:41`
    - `resolver/CorrelatedSubselects.java:2064,2073`
    - `sql/dialect/AnsiSqlRenderer.java:1379`
  - `String.format` with `%d` and the default locale:
    - `values/PureTimeLiteral.java:54,65,77`
    - `values/PureDateLiteral.java:97-103,250,298-350`
    - `lineage/ScanRelations.java:433,465`
    - `lowering/AsorRef.java:44,62`
    - `sql/Json.java:207`
- **Effect:** under `tr` (dotless-i case mapping) or locales with non-ASCII digits, SQL, plans and cached action outputs change.
- **Fix:**
  - Use `Locale.ROOT` in code.
  - Turn on Error Prone's `StringCaseLocaleUsage`/`DefaultLocale` as errors in the shared javacopts.
  - Pin `-Duser.language=en -Duser.country=US` in `junit_test` and `java_run`.

### N9 (P2). A test flips a process-wide property that changes core's SQL generation

- **Evidence:**
  - `core/src/main/java/com/legend/sql/dialect/DuckDb.java:269` reads `Boolean.getBoolean("legend.exec.engineScanOrder")` on every call.
  - `spec/src/test/java/com/legend/rcorpus/MinimalCorpusTest.java:138-146` sets and clears it at run time.
- **Effect:** any other code in that JVM during the window plans differently. This is test-order coupling, and the target does not declare the setting.
- **Fix:** give the corpus its own `junit_test` with `jvm_flags=["-Dlegend.exec.engineScanOrder=true"]`, or pass the option through the dialect's configuration object.

### N10 (P2). Product code carries more than 20 debug env switches, some write files to arbitrary paths, and one reads Bazel's test-output variable

- **Behaviour-changing or file-writing switches:**
  - `probe/Shadow.java:88-96` writes `TEST_UNDECLARED_OUTPUTS_DIR/shadow.tsv`, or to whatever path `LL_SHADOW` names.
  - `builtin/DecisionProbe.java:81-89` throws when `LL_SHADOW` is set without a binding.
  - `exec/PrepTrace.java:18,57-63` appends to any path in `LEGEND_LITE_PREP_TRACE`.
- **Counters and trace prints:** `StampCensus.java:43`, `TdsCompare.java:320`, `Equality.java:82`, `StatementExecutor.java:2537,2550`, `PlanAllocations.java:246`, `DatabaseJudge.java:227`, `Executor.java:113,215,318`, `WireTypes.java:137`, `CanonicalRenderSql.java:337`, `Lowerer.java:1977`, `Typer.java:1543`, `Overloads.java:413`, `InferenceKernel.java:1291`, `TemporalFrame.java:719,904,1331`, `StoreResolver.java:639`, `Anchors.java:405`, `NavMaterializer.java:1056,1086`, `Substitution.java:1433`, `ScanRelations.java:1020,2601`, `ServiceTestRunner.java:244`, `PureTestRunner.java:425,524`.
- **System properties:** `SpecCompiler.java:102` (`legend.spec.trace`), `MappingResolutionException.java:27` (`legend.mapping.trace`).
- **Driven from outside the graph:** `tools/census/lanes.sh:10-19` runs a nested `bazel test --test_env=LEGEND_LITE_DUMP_SQL=1 --test_env=LL_PCT_CASES=1` and scrapes `bazel info bazel-testlogs`. This is outside this slice but is the consumer of these switches.
- **Fix:**
  - Move probes and traces into test-only targets, or configure them through an explicit option object.
  - Express the census as Bazel test targets with an `env` attribute, writing to `TEST_UNDECLARED_OUTPUTS_DIR`, collected by a rule.
  - Product code should not read `TEST_UNDECLARED_OUTPUTS_DIR`.

### N11 (P2). The JDBC driver's service registration ships in the server jar and the native image, which do not contain the driver class

- **Evidence:** `warehouse/BUILD.bazel:60-61` globs all of `src/main/resources/**` into `:server_lib`. That includes `META-INF/services/java.sql.Driver`, which names `com.legend.warehouse.client.jdbc.WhDriver`. That class lives only in `:client` (`BUILD.bazel:34`).
- **Effect:** `DriverManager` silently swallows the resulting `ServiceConfigurationError`.
- **Fix:** `:server_lib` resources = `glob(["src/main/resources/META-INF/native-image/**"])`, or separate resource roots per library.

### N12 (P2). Native (FFM) access is not declared for the JVM targets

- **Evidence:** only `native_image` passes `--enable-native-access=ALL-UNNAMED` (`BUILD.bazel:109`). `//warehouse:server` (`:65-69`) and all warehouse `junit_test` targets do not, yet they call restricted `Linker` and `SymbolLookup.libraryLookup` (`Duck.java:38,77`).
- **Effect:** JDK 25 warns, and a later JDK will refuse. Behaviour follows the JDK's default, not the build.
- **Fix:** add `jvm_flags=["--enable-native-access=ALL-UNNAMED"]` to the server and the tests.

### N13 (P2). The rule that sqlapi must compile to WebAssembly is checked only by a dependency query, not by compiling it

- **Evidence:**
  - `warehouse/BUILD.bazel:11-14` states the rule; `tools/deps/BUILD.bazel:55-64` (`warehouse_closure_test`) is the only check.
  - No `teavm_wasm` target includes `//warehouse:sqlapi`: `wasm/BUILD.bazel:46-55` compiles only `:boundary`.
  - `wasm/README.md:58-69` itself explains that only the real TeaVM compile catches class-library gaps.
- **Fix:** add a `teavm_wasm` target over a small entry class that reaches `NativeBinding`, `ApiJson` and `ArrowIpcReader`, built in the gate chain.

### N14 (P2). Stale Maven-era references (no `pom.xml` exists anywhere in the tree)

- `tools/teavm/TeaVmCompile.java:31` cites `research/wasm/pom.xml`, which does not exist.
- `base/src/main/java/com/legend/base/Nullable.java:13` cites `core/pom.xml` for the NullAway flag, which now lives in `//tools/nullaway`.
- `core/README.md:22,322,329,393` gives `mvn -pl core test`, `core/pom.xml` and Maven module wiring.
- `core/BUILD.bazel:314,393` name lanes "Maven gate 1/10".
- `wasm/README.md:80` cites `research/HANDOFF…`.
- **Fix:** rewrite these to name the Bazel targets.

### N15 (P2, P0 if `bazel test //...` must be hermetic). The native image is linked by the host's C toolchain, and it runs in the default test set

- **Evidence:**
  - `third_party/rules_graalvm_command_line_tools.patch`: on a Mac without Xcode, "native-image runs as it does on Linux and finds the C toolchain through xcrun itself". `MODULE.bazel:296-299` applies the patch.
  - On Linux, native-image needs host gcc, glibc headers and static zlib through the auto-configured local cc toolchain.
  - `//warehouse:tests_native` (`BUILD.bazel:177-192`) is not tagged manual, so `bazel test //...` needs host Xcode/CLT or gcc plus zlib-dev, undeclared.
- **Fix:** register a hermetic cc toolchain with a sysroot that includes zlib (toolchains_llvm or hermetic_cc_toolchain) for `native_image`. At minimum, constrain the native targets with `exec_compatible_with` on a declared toolchain constraint so they skip cleanly instead of failing on hosts that lack it.

### N16 (P2). Platform selects have no default, and one download uses http

- **Evidence:**
  - `warehouse/BUILD.bazel:167-171` (`duckdb_library`) and `warehouse/defs.bzl:42-47` (`POSTGRES_EXTENSION`) cover only four os/cpu pairs and have no `//conditions:default`.
  - Other platforms get a select error instead of being declared incompatible; it was not confirmed whether `target_compatible_with` short-circuits that error.
  - `MODULE.bazel:346` downloads with `http://`. The download is sha-pinned, so integrity holds.
- **Fix:** derive `target_compatible_with` from the same keys (default `@platforms//:incompatible`), and use https.

### N17 (P2). Runfiles are found by working-directory-relative paths instead of the runfiles library

- **Evidence:**
  - `WarehouseArrowTest.java:125` uses `Path.of("warehouse/src/test/python/arrow_matches_json.py")`.
  - `WAREHOUSE_BINARY`/`WAREHOUSE_DUCKDB_LIBRARY` are passed as `$(rootpath …)` (`BUILD.bazel:139-140,186-187`) and used as relative paths (`TestServer.java:66,97,102`).
  - This works only when the runfiles tree is the working directory; manifest-only runfiles (Windows default) break it.
- **Fix:** use `$(rlocationpath …)` with `com.google.devtools.build.runfiles.Runfiles`.

## Section 2: KNOWN items re-confirmed, with added evidence

- **K8.** `warehouse/defs.bzl:53-59` unpacks the extension with `run_shell` (`gzip -dc "$1" > "$2"`), a host tool. `defs.bzl:69-81` is the bash launcher.
  - Its runfiles handling is hand-rolled: `here="${RUNFILES_DIR:-$0.runfiles}/_main"`, falling back to `$(pwd)`.
  - The hard-coded `_main` breaks if the rule is used from another module or with manifest-only runfiles. It also `cd "${BUILD_WORKING_DIRECTORY:-.}"`.
  - Shared by `//warehouse:serve` (`BUILD.bazel:197-203`) and `//datacube:app` (`datacube/BUILD.bazel:713-725`).
  - **Fix:** do the gunzip in a `java_run` action (`GZIPInputStream`). Write the launcher in Java with the runfiles library, or let the server resolve runfiles itself, and pair it with N4's package.
- **K14.**
  - `WarehouseArrowTest.java:99,140-152` probes `python3`/`python` on the host PATH and calls `Assumptions.abort` unless `WAREHOUSE_ARROW_CHECK=required`.
  - The script is declared as data (`BUILD.bazel:90,123,134,181`) but the interpreter and pyarrow are not.
  - CI: `.github/workflows/gates-run.yml:123-128` runs `pip install --target $RUNNER_TEMP/pyarrow pyarrow==23.0.1`, and `:140-142` passes `--test_env=PATH --test_env=PYTHONPATH=… --test_env=WAREHOUSE_ARROW_CHECK=required`.
  - `WarehousePostgresLiveTest.java:79` reuses the same checker.
  - **Fix:** a rules_python hermetic interpreter, `pip.parse` with locked pyarrow, and a `py_binary` checker as data; or an Arrow-Java reader from `@maven_test`.
- **K18.**
  - `warehouse/BUILD.bazel:116-146`: `postgres_live` and `postgres_live_native` are manual, with a hand-supplied `LEGENDLITE_PG_DSN` and `LEGENDLITE_PG_EXTENSIONS=/abs/dir` (`WarehousePostgresLiveTest.java:33-56`).
  - The BUILD comment's precondition ("manual until embedded Postgres (P2) is in the test environment") is now met: `@embedded_postgres` (`MODULE.bazel:107-111`) and `testing/EmbeddedPostgres.java` exist, and the extension is already in the graph (`MODULE.bazel:339-353`). Only the gunzip, trapped inside `warehouse_run`, blocks it.
  - **Fix:** turn the gunzip into its own target, and make the live tests non-manual with `-Dembedded.postgres.root=$(rlocationpath …)` plus the extension as data.
- **K11.** `gates-run.yml:66-70` filters the native lane off Windows with jq, duplicating `target_compatible_with` (`BUILD.bazel:100-103`). Lines 123-142 are covered under K14.
- **K2.** `fixtures/saved-queries/make.mjs:2-5`: `java -jar bazel-bin/core/server_deploy.jar 18777 --query-store <empty dir> &` uses host java and a fixed port. The server side is `LegendHttpServer.java:358-379` (`PORT` env, default 8080, `--query-store`) and `SavedQueries.java:27,104`.
  - **Fix:** a test target that starts `//core:server` on port 0, reads the "started on port N" line (`LegendHttpServer.java:347`), and writes outputs as diff-tested files.
- **K15.** `core/BUILD.bazel:266-275`: `_CORE_READS = glob(["src/**"])` is data for every lane, plus whole-module `:srcs` for census and stress.
- **K19 (cross-slice note).** `tools/census/lanes.sh`: a nested bazel invocation, `runs/` writes and testlog scraping (see N10).
- **K12** (`testing/Repo.java`) is outside this slice. In-slice examples of the same hand-rolled pattern are in N17.

## Section 3: Coverage

**Read in full:**

- **warehouse build and resources:** `BUILD.bazel`, `defs.bzl`, `reachability-metadata.json`, `META-INF/services/java.sql.Driver`.
- **warehouse server:** all 13 files in `server/*.java` (`WarehouseServer`, `Statements`, `Attachment`, `AdminStatements`, `Authorizer`, `Catalogs`, `Grants`, `History`, `Identity`, `Postgres`, `PostgresUrl`, `ResultEncoder`, `ResultStore`, `Sessions`) and all 14 files in `server/duck/*.java`.
- **warehouse API and client:** `sqlapi/{NativeBinding, SqlApi, SqlApiBinding}`, `client/WarehouseClient`, `client/jdbc/WhDriver`.
- **warehouse tests:** `TestServer`, `WarehouseArrowTest`, `WarehouseJdbcJsonTest`, `src/test/python/arrow_matches_json.py`.
- **wasm and TeaVM:** everything in `wasm/` (`BUILD.bazel`, `README.md`, `differential.mjs`, `zoneprobe.mjs`, `startup.mjs`, `JvmMain`, `ZoneMain`, `PreludeResources`, the ResourceSupplier services file; corpus listed) and everything in `tools/teavm/` (`BUILD`, `defs.bzl`, `TeaVmCompile.java`).
- **base, json, core:** all of `base/` (`BUILD`, `Nullable`, `NonNull`), `json/BUILD.bazel`, `core/BUILD.bazel`.
- **Related files, also read:** `third_party/rules_graalvm_command_line_tools.patch`, `MODULE.bazel` (graalvm, maven, extension and postgres sections), `.bazelrc`, `tools/junit/defs.bzl`, `tools/java_run/defs.bzl` (head), `tools/census/lanes.sh`.

**Partly read:**

- `wasm/src/main/java/planner/Wasm.java` (grep only)
- `WarehouseServerTest` (40-70, 280-412)
- `WarehouseJdbcTest` (1-80)
- `WarehousePostgresLiveTest` (40-70, 185-221)
- `AppModeTest` (75-100, 186-200)
- `PostgresCatalogTest` (55-80)
- `WarehouseEntitlementsTest` (270-300)
- `ConnectionResolver` (160-240)
- `LegendHttpServer` (340-410)
- `DecisionProbe`, `Shadow`, `PrepTrace`, `DuckDb` (255-290, 375-400)

**Swept by grep, not read line by line:**

- `sqlapi/{ApiJson, ApiValues, ArrowIpcReader, Columnar, DuckType, Intervals}`.
- `client/jdbc/{Carriers, Unsupported, WhArray, WhBlob, WhConnection, WhDatabaseMetaData, WhResultSet, WhResultSetMetaData, WhStatement, WhStruct}`. The only hit was `ZoneId.systemDefault` at `Carriers.java:185`, which is covered by tests' `TZ=GMT`.
- The rest of `WarehouseCorsTest`, `IdentityTest` and the remaining test bodies.
- `json/Json.java` and `JsonTest.java`: pure in-memory; only hit `Locale.ROOT` at `Json.java:734`.
- All of `core/src/main`: 746 files, 742 Java files totalling 221,437 lines, plus the `duckdb/` services file and four resources.

**Exact sweep patterns (`grep -rn -E`):**

1. `System\.getenv|System\.getProperty|Boolean\.getBoolean|Integer\.getInteger|Long\.getLong|System\.getProperties`
2. `Files\.|Path\.of|Paths\.get|new File\(|FileInputStream|FileOutputStream|FileReader|FileWriter|RandomAccessFile|createTemp|deleteOnExit|java\.io\.tmpdir|user\.dir|user\.home|toAbsolutePath|getResource|ServiceLoader|ProcessBuilder|Runtime\.getRuntime|\.exec\(`
3. `Class\.forName|DriverManager|jdbc:|HttpClient|HttpServer\.create|new Socket|ServerSocket|InetSocketAddress|new URL\(|URI\.create|openConnection|loadLibrary|System\.load\(|java\.class\.path|getContextClassLoader|ClassLoader`
4. `ZoneId\.systemDefault|TimeZone\.getDefault|TimeZone\.setDefault|Locale\.getDefault|Locale\.setDefault|Charset\.defaultCharset|Clock\.systemDefault|new Date\(\)|SimpleDateFormat|String\.format\(|\.toUpperCase\(\)|\.toLowerCase\(\)|getBytes\(\)|System\.lineSeparator|File\.separator|os\.name|availableProcessors|currentTimeMillis|nanoTime`, plus `String\.format\(|\.formatted\(`
5. `"target/|target/classes|src/main/|src/test/|pom\.xml|\bmvn\b|surefire|Maven|maven` (over `core/src/main/java` and `duckdb`)
6. For the warehouse client and API: `getenv|getProperty|getBoolean|getInteger|Files\.|Path\.of|new File|FileInputStream|getResource|ServiceLoader|ProcessBuilder|exec\(|HttpClient|Socket|URL\(|URI\.create|forName|class\.path|user\.(dir|home|name)|tmpdir|loadLibrary|System\.load|TimeZone|Locale\.getDefault|ZoneId\.systemDefault|Charset\.defaultCharset`
7. For warehouse tests: `getenv|getProperty|createTemp|Path\.of|Files\.|ProcessBuilder|@TempDir|port|Thread\.sleep|Assumptions|@Disabled|@Tag|@Order|LEGENDLITE|WAREHOUSE_`

**Core sweep result:** apart from N5, N8, N9 and N10, core main has no `ProcessBuilder`, no native loading, no `java.class.path`, `user.dir` or `user.home` use, no `ZoneId.systemDefault`, and no runtime Maven or `target/` paths. Resources load through `getResourceAsStream` (`Prelude.java:42`, `EngineHandlers.java:73`). `ServiceLoader` is used at `SectionGrammarRegistry.java:114`, `Executor.java:60` (BulkLoad, bound by `:duckdb_load`) and `DecisionProbe.java:85`.

**Not done:** no Bazel command was run, so N16's select behaviour and the C toolchain rules_graalvm actually picks were not confirmed; its external repository was not cached on the audit machine.
