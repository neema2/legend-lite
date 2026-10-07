# G5 dossiers: the JS-side and runtime generators

Repo: `the build/rebuild checkout` (branch build/rebuild, HEAD `669b39ad1`). Paths are repo-relative.
`DC` = `datacube/BUILD.bazel`, `EC` = `engine-client/BUILD.bazel`, `LA` = `legend-art/BUILD.bazel`, `W` = `wasm/BUILD.bazel`,
`WH` = `warehouse/BUILD.bazel`, `ROOT` = `BUILD.bazel`, `C/` = `core/src/main/java/com/legend/`.

**Method.** Every claim below was read from the source or the build graph on 2026-10-05.
- "aq" = `env -C <repo> bazel aquery --output=jsonproto '<mnemonic>(…, <label>)'`, summarized per action by input category
  (scripts in the session scratchpad, `g5/jr.py`, `g5/aq2.py`).
- "q" = `env -C <repo> bazel query`. "rdeps1" = `q 'rdeps(//..., <label>, 1)'`.
- "closure" = a static walk of the relative `import`/`export … from`/`import()` statements of a TS/JS entry point, with
  the regex `datacube/tools/test-imports.mts:17` uses (scratchpad `g5/clos.py`, `g5/bare.py`). Bare (npm) imports are
  listed separately.
- Core library of a class = `q 'kind(java_library, rdeps(//core:*, //core:<file>, 1)) except //core:core_next'`.

**Shared facts.**
- **java_run** (`tools/java_run/defs.bzl:44-120`) puts every `transitive_runtime_jars` of its `deps` on the class path
  and into the action's inputs (`:46,110`). It adds the exec JDK (118 files of `remotejdk25`, aq) and pins
  `-Duser.timezone=GMT -Duser.language=en -Duser.country=US -Dfile.encoding=UTF-8 -Djava.io.tmpdir=<name>_tmp`
  (`:85-91`). It sets `-Xmx` and a scheduler resource_set only with `memory_mb` (`:92-114`). **None of the 8 java_runs in
  G5 sets `memory_mb`**, so each runs at the JVM's default heap with no resource_set.
- **The //core umbrella** (`core/BUILD.bazel:235-239`) exports `_CORE_TARGETS` (`:210-215`): 31 core libraries plus `//base`
  and `//json`. Its runtime closure is 33 jars. Any edit under `core/src/main/java` or `core/src/main/resources` (the four
  resources ride in `//core:builtin`) changes at least one of them.
- **Closure sizes** (`q 'kind(java_library, deps(X)) intersect //...:*'`, counting base and json):
  - `//core` 34 (33 jars plus the umbrella itself, which has no jar);
  - `//core:planner` 24;
  - `//core:plan_side` 26 jars;
  - `//core:lowering` 15;
  - `//core:compiler_element_type` 13;
  - `//core:database` 9, `//core:parser` 9;
  - `//core:protocol` 4;
  - `//core:sql_dialect` 3;
  - `sql_dialect + database + parser + protocol + //json`: 12.
- **The guard rules do not run generators.** `guard_classpaths` reads only `JavaRuntimeClasspathInfo`
  (`tools/guards/classpath.bzl:42-50`), and `guard_markdown` reads only runfiles at analysis time
  (`tools/guards/markdown.bzl:9-18`). They appear in rdeps1 below, but depending on them does not execute a generator's
  action.
- **The committed-file suites.**
  - `//:generated` (`ROOT:79-97`) is in `//gates:local` (`gates/BUILD.bazel:15`) and in CI's `checks` lane
    (`.github/workflows/gates-run.yml:51`) on Linux, macOS, Windows and Linux arm64 (`.github/workflows/gate.yml:65-96`).
  - `//:update_generated` (`ROOT:105-125`) is testonly and not manual, so `bazel build //...` builds every writer's
    generator. `tools/bump/Bump.java:149-151` runs it on every upstream bump, then `bazel test //...` (`:154-157`).
- **CI lanes that matter here** (`gates-run.yml`):
  - `checks` (`:51`);
  - `app` (`:61`): `//datacube:tests //datacube:verify_app_test //wasm:all //warehouse:tests …`;
  - `build` (`:63`, `bazel build //...`);
  - `native` (`:64`): `//warehouse:tests_native //warehouse:launcher_test //datacube:app`;
  - `browser` (`:65`, Linux only).
- **None of the 16 is tagged `manual`.** `reachability_metadata` is the only testonly one (q `attr(tags, manual, …)`,
  `attr(testonly, 1, …)`). So all 16 are built by the `build` lane and by any local `bazel build //...`.

---

## 1. //datacube:catalog_rules

1. **Identity.**
   - A `java_run` at `DC:378-384`, args `["rules", "{OUT}"]`.
   - Program: `catalogfacts.CatalogFacts` (`datacube/tools/catalogfacts/CatalogFacts.java`). `main` is at `:52-58` and
     the mode at `:65-130`.
   - Library `:catalog_facts_main` (`DC:365-376`): deps `//core`, `//json`, and runtime_deps
     `@duckdb_jdbc_warehouse//jar`.
2. **What it computes.** It writes each database's catalog-type decisions as TypeScript data:
   - DuckDB's and Postgres's `CatalogRules` (types, aliases, refused types, decimal rule), each distinct `CatalogType`
     once;
   - plus the one SQL question the tab asks DuckDB's catalog (`DuckDb.CATALOG_COLUMNS_SQL`).

   DataCube's TypeScript model writer runs `CatalogRules.typeOf`'s algorithm over this data, so it adds no type rule of
   its own (`CatalogFacts.java:24-46`).
3. **Why it exists.**
   - Introduced by `aacbf7eb5` (2026-10-01, "Structured DuckDB catalog: one model writer, generated into TypeScript"),
     after the user's ruling of 2026-10-01: the tab writes a table's model in TypeScript, with no WebAssembly on any
     planner (`DC:361-364`).
   - It protects DataCube's TS catalog writer from drifting from core's dialect decisions. The reason still holds.
4. **Inputs, declared** (aq, 153 inputs).
   - 34 jars of ours: all 31 core libraries, `base`, `json`, and `catalog_facts_main`.
   - 1 external jar: `+product_jars+duckdb_jdbc_warehouse/duckdb_jdbc-1.5.5.1.jar`, the http_jar from
     `tools/deps/jars.bzl:33-37`.
   - 118 JDK files.
   - No source files and no other generator's outputs.
5. **Inputs, actually read.**
   - `rules()` touches only `CatalogType.Read`, `DuckDb.CATALOG_RULES`, `Postgres.CATALOG_RULES` and
     `DuckDb.CATALOG_COLUMNS_SQL` (`:69,83,128`), all in `//core:sql_dialect`. It writes `args[1]` (`:57`), and it reads
     no file, environment variable or system property.
   - **Over-declared:** 28 of the 31 core jars (all but `sql`, `sql_dialect` and their dependency `base`), `json`, and
     the 77 MB DuckDB JDBC jar (never touched in `rules` mode).
   - Under-declared: none.
6. **Outputs.** `datacube/generated/catalog-facts.gen.ts`: the `CatalogRead` type, the `CatalogType` and `CatalogRules`
   interfaces, the named decisions, `CATALOG_RULES` for DuckDB and Postgres, and `CATALOG_COLUMNS_SQL`. A scratch dir
   `catalog_rules_tmp` is also output.
7. **Committed?** Yes.
   - The file is `datacube/src/generated/catalog-facts.ts` (9,214 bytes).
   - Its writer is `//datacube:update_generated` (`DC:421-431`, entry `src/generated/catalog-facts.ts`), and its diff
     test is `//datacube:update_generated_1_test` (rdeps1).
   - The test suite is `//datacube:update_generated_tests`, which is in `//:generated` (`ROOT:85`). The writer is in
     `//:update_generated` (`ROOT:112`).
8. **Who consumes it.**
   - **Product at run time:** `datacube/src/catalog-model.ts:17` imports it. `infer.ts:28` and `upload.ts:12` import
     catalog-model, and `wasm-planner.ts:29` imports it type-only. The closure of each shipped entry includes
     catalog-facts.ts: `datacube/demo/main.ts`, `datacube/demo/page.ts` and `query/demo/main.ts` (the Query app ships
     DataCube's sources).
   - **Tests:** `datacube/test/typed-values/typed-values.ts:24` (the typed_values tests), plus every node_test whose
     import closure reaches catalog-model (`datacube/test_imports.bzl`).
   - The link dictionary tool's closure includes it (§6).
   - **Humans:** `DC:423`, the diff message "regenerate: bazel run //datacube:update_generated".
9. **Determinism.** Yes.
   - The maps are `LinkedHashMap` copies of `LinkedHashMap`s (`C/sql/dialect/CatalogRules.java:34-36`;
     `DuckDb.java` `catalogTypes()`/`catalogRefused()` use `LinkedHashMap`). Aliases are sorted through a `TreeMap`
     (`CatalogFacts.java:116`).
   - There is no time, randomness or path in the output. One committed copy passes `//:generated` on 4 platforms.
10. **Cost.** One small JVM and pure string building. It starts no database, server or DuckDB.
11. **What reruns it today.**
    - Any change to the 33 core/base/json jars: any core source or resource edit, including `prelude.pure`,
      `engine-handlers.tsv`, `native-claims.tsv` and `native-membership.tsv`.
    - `CatalogFacts.java`, the DuckDB 1.5.5.1 jar pin, the JDK, and `java_run`.
    - Evidence: `q 'somepath(//datacube:catalog_rules, //core:src/main/java/com/legend/server/LegendHttpServer.java)'`
      gives `catalog_rules → catalog_facts_main → //core:core → //core:server_lib → LegendHttpServer.java`.
12. **What SHOULD rerun it.**
    - A change to core's dialect catalog decisions: `C/sql/dialect/{CatalogRules,CatalogType,DuckDb,Postgres}.java`, in
      `//core:sql_dialect` (closure 3).
    - `CatalogFacts.java`.
13. **Who runs it today.**
    - `bazel build //...` (the CI `build` lane), and `//gates:local` through
      `//:generated → //datacube:update_generated_tests → update_generated_1_test` (q `somepath(//gates:local, …)`).
    - The CI `checks` lane, and the bump (`//:update_generated`, then `test //...`).
    - By hand: `bazel run //datacube:update_generated` (`DC:423`, `CatalogFacts.java:44-45,63`).
14. **Recommendation: COMMITTED-SOURCE.** The output is shipped product data that mirrors named core files.
    - Manual: yes. Its diff test still builds it.
    - Writer: `//:update_generated` (group C/D).
    - Diff test: the everyday gate.
    - Narrowing: give `rules` its own main library with `deps = ["//core:sql_dialect"]`, closure `base + sql +
      sql_dialect`. Today one library serves both modes; the fix is to split `catalog_facts_main` in two, or keep one
      library on the 12-library set in §2.
15. **Open questions.** None.

## 2. //datacube:catalog_corpus

1. **Identity.**
   - A `java_run` at `DC:386-392`, args `["corpus", "{OUT}"]`.
   - Same program and library as §1. The mode is at `CatalogFacts.java:180-289`.
2. **What it computes.**
   - It builds a fixed corpus of about 65 table cases (`:181-223`):
     - 44 single-column DuckDB types (`:184-192`);
     - mixed, awkward and read-only tables (`:193-212`);
     - Postgres tables given as the catalog rows Postgres 17.11 answered on 2026-10-02 (`:162-178,213-219`);
     - two hand-given rows (`:220-223`).
   - It **starts an in-memory DuckDB** (`DriverManager.getConnection("jdbc:duckdb:")`, `:239`). In it, it creates each
     real table and reads its columns back with `DuckDb.CATALOG_COLUMNS_SQL` (`:291-309`).
   - It then records what `CatalogModel.database` answers for each case (`:259-261`): the Database text, the accessor
     and its protocol JSON (parsed by `SpecParser.parseLambda`, emitted by `ProtocolEmitter.emitLambda`, stripped by
     `SourceInformation.strip`, `:263-265`), the conversions and exclusions, or the refusal's message (`:280-281`).
3. **Why it exists.** Same commit as §1 (`aacbf7eb5`). DataCube's TS writer `src/catalog-model.ts` is tested against
   these answers (`datacube/test/catalog-model.test.ts`): "any drift fails the build" (`CatalogFacts.java:40-41`).
4. **Inputs, declared** (aq). The same 153 as §1: 34 jars of ours, DuckDB JDBC 1.5.5.1, 118 JDK files.
5. **Inputs, actually read.**
   - Classes used:
     - `com.legend.json.Json` (`//json`);
     - `SpecParser` (`//core:parser`);
     - `ProtocolEmitter` and `SourceInformation` (`//core:protocol`);
     - `CatalogModel` and `DuckDb` (`//core:sql_dialect`);
     - `com.legend.database.Databases` (`//core:database`, `:260`).
   - Their runtime closure is 12 libraries: base, json, error, lexer, model, spi, values, protocol, parser, sql,
     sql_dialect, database.
   - It needs DuckDB JDBC 1.5.5.1 and its bundled native library, which DuckDB's JDBC driver extracts at load time,
     presumably into `java.io.tmpdir` (the declared `catalog_corpus_tmp`).
   - It writes `args[1]`.
   - **Over-declared:** 21 core jars: builtin, cache, compiler, compiler_element_type, diagnostics, driver, exec,
     execution_plan, ide, lineage, lowering, normalizer, plan, planner, platform, probe, resolver, server_lib, test,
     testdatagen, validation.
   - Under-declared: none known (see open question 1).
6. **Outputs.** `datacube/generated/catalog-corpus.gen.ts`: `CorpusColumn` and `CATALOG_CORPUS`, each case's input
   columns and expected answer or error (56,506 bytes committed). Plus the tmp dir.
7. **Committed?** Yes.
   - The file is `datacube/test/generated/catalog-corpus.ts`.
   - Writer: `//datacube:update_generated` (`DC:427`). Diff test: `//datacube:update_generated_2_test`.
   - Suites: `//:generated` and `//:update_generated`, as §1.
8. **Who consumes it.**
   - **Tests only:** `datacube/test/catalog-model.test.ts:11` (`//datacube:catalog_model_test`, which has it as data,
     `DC:155`).
   - No product import: the closures of `demo/main.ts`, `demo/page.ts` and `query/demo/main.ts` do not contain it.
9. **Determinism.**
   - Same inputs give the same bytes, given the pinned DuckDB jar. The cases are a fixed ordered list, the output
     follows list order, and DuckDB's catalog rows are ordered by `CATALOG_COLUMNS_SQL`.
   - The bytes depend on DuckDB 1.5.5.1's own answers (type names, precision), so a DuckDB bump moves them.
   - One copy passes on 4 platforms.
10. **Cost.**
    - One JVM. **It starts DuckDB** (in-memory, about 65 `CREATE TABLE` and catalog reads) and loads its 77 MB jar and
      native library. No server.
    - It is the only generator in G5, and the only one in the design's list (R3), that runs a database at build time.
11. **What reruns it today.**
    - The same as §1: every core jar, the DuckDB pin, the JDK.
    - `q 'somepath(//datacube:catalog_corpus, //core:src/main/java/com/legend/probe/Shadow.java)'` gives
      `catalog_corpus → catalog_facts_main → //core:core → //core:probe → Shadow.java`.
12. **What SHOULD rerun it.**
    - **Engine behaviour:** `CatalogModel.database`, the dialects, `Databases`, the parser and protocol emitter, the 12
      libraries above.
    - DuckDB's catalog answers, which change with the `duckdb_jdbc_warehouse` pin.
    - `CatalogFacts.java`'s corpus.
13. **Who runs it today.**
    - `bazel build //...`, and `//gates:local` (through `//:generated → update_generated_2_test`).
    - The `checks` lane, and the bump.
    - By hand: `bazel run //datacube:update_generated`.
14. **Recommendation: TEST-IN-DISGUISE.**
    - It pins engine behaviour (CatalogModel's answers over a real DuckDB) as a golden, and its diff test fails when
      that behaviour moves. That is principle 4 of the design: "anything that pins engine behaviour is a test".
    - The natural shape is the differential pattern used by `cube_jvm_answers`: a **testonly, uncommitted build output**
      read by `catalog_model_test`, so the test compares the TS writer with the Java writer live. rules_js lays outputs
      beside sources, so `test/generated/catalog-corpus.ts` can be the output's path.
    - Manual: yes; only `catalog_model_test` reaches it.
    - Writer: none. Or, if a committed golden is kept, an explicit re-pin outside `//:update_generated` (the ladder
      pattern).
    - The test belongs in the everyday DataCube suite.
    - Narrowing: depend on `//core:sql_dialect`, `//core:database`, `//core:parser`, `//core:protocol` and `//json`
      (12 libraries), with runtime `@duckdb_jdbc_warehouse//jar`.
15. **Open questions.**
    1. Does DuckDB write or read anything outside the action's scratch directory (for example `~/.duckdb` for extension
       autoload of `JSON`)? To settle it, run the action once with `--sandbox_block_path=$HOME` (or read DuckDB JDBC
       1.5.5.1's `DuckDBNative` and its extension-loading defaults).
    2. Is a TS-side test that depends on a JVM+DuckDB run acceptable in the everyday DataCube suite, or should it live in
       the `app` lane only? That is the user's call.

## 3. //datacube:offer_queries

1. **Identity.**
   - A `js_run_binary` at `DC:323-335`, outputs `offer_model.pure` and `offer_queries.tsv`.
   - Tool: `:emit_offer_queries`, a js_binary at `DC:278-286` with data `[":src"]` and node `--experimental-strip-types`.
   - Program: `datacube/tools/offer-facts/emit.ts`; the work is at `:137-164`.
2. **What it computes.**
   - It writes a Pure model (table `offer::DB.T` with one probe column per DuckDB type and its twin, plus `TRADES`,
     `:82-97`).
   - It writes a TSV of exactly the queries DataCube sends, built by the **product's own query builder**
     (`src/query.ts` `levelLambda`):
     - one `column` line per probe;
     - one `type` line;
     - an `agg` line per probe and aggregate (13 aggregates);
     - an `op` line per probe and filter operator (31 operators);
     - a `calc` line per curated calculated-column function (22, from `src/calc.ts:75-99`).
   - With 13 probes that is about 595 lines (counts from the committed `offer-facts.ts`).
3. **Why it exists.** Introduced by `05aef9bde` (2026-09-28, "DataCube T5, first half…"), for
   `docs/DATACUBE_TYPES_TO_SERVER_2026_09_27.md` T5. It feeds `offer_facts` (§4).
4. **Inputs, declared** (aq, 252 inputs).
   - Sources: 87 `datacube/src` files, 14 `engine-client/src`, 10 `pure-protocol/src`, 6 `query-store/src`, 4
     `package.json` files, `tsconfig.json`, and `emit.ts` (source and bin copy).
   - 122 npm package files: duckdb-wasm, apache-arrow, echarts, fflate, @swc/helpers, and their dependencies.
   - The tool launcher and its runfiles tree, which carries Node.
5. **Inputs, actually read.**
   - The import closure of `emit.ts` is 33 files:
     - 13 in `datacube/src`: calc, cube, epoch, generated/offer-facts, grid/columns, json-shape, plan, query, runner,
       snap, snapshot, tree, treeview;
     - 10 in `engine-client/src`, including `generated/lite-facts.ts`;
     - 10 in `pure-protocol/src`.
   - Its only bare import is `node:fs`, and it writes its two argv paths.
   - **Over-declared:** 74 of 87 datacube sources, 4 of 14 engine-client sources, all 6 query-store sources, and all 122
     npm files.
   - The committed `src/generated/offer-facts.ts` (§4's own output) is in the closure through `calc.ts:30`. But
     `CALC_FUNCTIONS` is a literal (`calc.ts:75-99`) and the TSV uses only `f.name` and `f.example`, so regenerating
     offer-facts.ts reruns this action with identical bytes.
6. **Outputs.** `datacube/offer_model.pure` (the probe model) and `datacube/offer_queries.tsv` (the query lines).
7. **Committed?** No. A build output consumed by `//datacube:offer_facts` (rdeps1).
8. **Who consumes it.** `//datacube:offer_facts` only. No test reads it directly, and nothing ships it.
9. **Determinism.** Yes. It uses fixed arrays, `Object.keys` of literal objects, and compact JSON (`emit.ts:162-164`).
   There is no time or randomness: the dates in the file are literals.
10. **Cost.** One Node process doing pure computation. No database, server or JVM.
11. **What reruns it today.**
    - Any edit to the 87 datacube, 14 engine-client, 10 pure-protocol or 6 query-store sources; the npm locks of
      datacube and engine-client; Node and rules_js.
    - `q 'somepath(//datacube:offer_queries, //engine-client:src/warehouse.ts)'` gives
      `offer_queries → emit_offer_queries → //datacube:src → src_base → //engine-client:engine_client → warehouse.ts`.
12. **What SHOULD rerun it.** `emit.ts` and its 33-file closure: the query builder, the snapshot types, calc's curated
    list, pure-protocol, and engine-client's type and result modules.
13. **Who runs it today.**
    - `bazel build //...`; `//gates:local` through `//:generated → update_generated_0_test → offer_facts`.
    - The `checks` lane, and the bump.
14. **Recommendation: BUILD-OUTPUT.**
    - It is an intermediate of `offer_facts`. It could be testonly once `offer_facts` is reached only by its diff test.
    - Manual: yes.
    - No writer; no diff test of its own.
    - Narrowing: give the tool data that is just its 33-file closure, generated as `TEST_IMPORTS` is (a "tool imports"
      list), with no npm packages. Design §4.6 ports these JS generators off Node.
15. **Open questions.** None.

## 4. //datacube:offer_facts

1. **Identity.**
   - A `java_run` at `DC:345-359`: srcs `offer_model.pure`, `offer_queries.tsv`; output
     `generated/offer-facts.gen.ts`.
   - Program: `offerfacts.OfferFacts` (`datacube/tools/offer-facts/OfferFacts.java`, `main` at `:60-115`).
   - Library `:offer_facts_main` (`DC:337-343`), deps `//core`.
2. **What it computes.**
   - It compiles DataCube's own queries with legend-lite's compiler (`Compiler.compileModel`, `:69`; `Compiler.query`,
     `:82,133,210,220`). It records:
     - for each type, each aggregate's measure type (or null when refused) and whether each filter operator's
       condition compiles;
     - for each curated calculated-column function, the path the compiler resolves the name to
       (`NameResolver.resolveQuery`, `:140`) and every declared overload's signature (`ModelContext.findFunction`,
       `:164`).
   - **It also judges.** Generation fails when a probe's compiled type differs from the type emit.ts built it as
     (`:101-103`), when two probes of one type disagree (`:107-110`), or when a curated example does not compile
     (`:137-138`).
3. **Why it exists.**
   - Introduced by `05aef9bde` (2026-09-28), with T5's filter operators completed in `4179729d6`.
   - It makes what DataCube offers a column (aggregates, filter operators, calculated-column signatures) come from the
     compiler, never from a hand list (`OfferFacts.java:31-46`).
4. **Inputs, declared** (aq, 154 inputs). 34 jars (all 31 core, base, json, offer_facts_main); the 2 outputs of
   `offer_queries`; 118 JDK files.
5. **Inputs, actually read.**
   - Classes used:
     - `Compiler` and `TypedQuery` (`//core:planner`);
     - `NameResolver`, `ModelContext`, `TypedFunction`, `TypedParameter`, `TypedNativeCall` and `TypedSpec`
       (`//core:compiler`);
     - `Type` (`//core:compiler_element_type`);
     - `UpstreamRelationType` (`//core:plan`);
     - `ProtocolReader`, `AppliedFunction` and `LambdaFunction` (`//core:protocol`).
   - Their runtime closure is `deps(//core:planner)`, 24 libraries. The planner reads `prelude.pure` and
     `engine-handlers.tsv` from `//core:builtin` (`C/builtin/Prelude.java:42`, `C/builtin/EngineHandlers.java:73`),
     which is inside that closure.
   - It reads the two argv files and writes the third.
   - **Over-declared:** 9 core jars: diagnostics, driver, exec, execution_plan, ide, probe, server_lib, test,
     testdatagen.
   - Under-declared: none.
6. **Outputs.** `datacube/generated/offer-facts.gen.ts`: the `OfferFact` interface, `OFFER_FACTS` keyed by type with its
   probe in a comment, and `CALC_FACTS` (path and signatures).
7. **Committed?** Yes.
   - The file is `datacube/src/generated/offer-facts.ts` (19,358 bytes).
   - Writer: `//datacube:update_generated` (`DC:425`). Diff test: `//datacube:update_generated_0_test`.
   - Suites: `//:generated` and `//:update_generated`.
8. **Who consumes it.**
   - **Product at run time:** `datacube/src/offers.ts:6` (`OFFER_FACTS`) and `datacube/src/calc.ts:30` (`CALC_FACTS`).
     It is in the closure of `datacube/demo/main.ts`, `demo/page.ts` and `query/demo/main.ts`.
   - **Tests:** `datacube/test/offer-facts.test.ts:9`, `calc.test.ts:21`, and every test whose import list contains it
     (`datacube/test_imports.bzl`, for example `adhoc-mode`).
   - **Generators:** `link_dictionary_next` reads `OFFER_FACTS` (`datacube/tools/link-dictionary/make.ts:20,46-47,96-97`),
     and `offer_queries`' closure contains it (§3).
   - **Humans:** `DC:423`; `calc.ts:106` throws "bazel run //datacube:update_generated".
9. **Determinism.** Yes. The maps are `TreeMap` by type (`:94`) and `LinkedHashMap` in TSV order; signatures go through a
   `TreeSet` (`:170`). Error messages are not in the output. One copy passes on 4 platforms.
10. **Cost.** One JVM: one model compile and about 595 query compiles. No database, server or DuckDB.
11. **What reruns it today.**
    - Every core jar; `OfferFacts.java`; any change to `offer_queries`' bytes; the JDK.
    - `q 'somepath(//datacube:offer_facts, //core:src/main/java/com/legend/exec/BulkLoad.java)'` gives
      `offer_facts → offer_facts_main → //core:core → //core:exec → BulkLoad.java`.
12. **What SHOULD rerun it.**
    - Our compiler's typing and function registry (the 24-library planner closure, which includes `Pure.java` and
      `prelude.pure`, so an upstream bump can move it too).
    - DataCube's query builder, through `offer_queries`.
    - `OfferFacts.java`.
13. **Who runs it today.**
    - `bazel build //...`; `//gates:local` (through `//:generated → update_generated_0_test`).
    - The `checks` lane, and the bump.
    - By hand: `bazel run //datacube:update_generated`.
14. **Recommendation: COMMITTED-SOURCE.**
    - The output is shipped product data. The import must be a committed file, or `//:web` would rerun the compiler on
      every core edit.
    - Its triggers are named files of ours (the planner closure, DataCube's query builder). It also carries an embedded
      check (the probe-type and example failures). Those failures are useful at regeneration time and need not become a
      separate test.
    - Manual: yes.
    - Writer: `//:update_generated`.
    - Diff test: the everyday gate (`//:generated`).
    - Narrowing: `offer_facts_main` deps `//core:planner`, `//core:compiler`, `//core:compiler_element_type`,
      `//core:plan`, `//core:protocol` (closure 24 libraries).
15. **Open questions.** None.

## 5. //datacube:test_imports

1. **Identity.**
   - A `js_run_binary` at `DC:409-419`, output `test_imports_generated.bzl`.
   - Tool: `:test_imports_tool` (`DC:395-402`) running `datacube/tools/test-imports.mts`, with no data.
   - srcs `:import_scan` = `glob(src/**/*.ts, test/**/*.ts)` (`DC:404-407`).
2. **What it computes.**
   - For each `test/<name>.test.ts`, the set of files under `src/` that its relative imports reach, through the test
     helpers too (`test-imports.mts:36-60`).
   - It fails if an import into `src/` or `test/` resolves to no declared file (`:28-31`).
   - Written as a Starlark dict `TEST_IMPORTS`.
3. **Why it exists.**
   - Introduced by `84690e4a5` (2026-10-05, "DataCube tests depend on what they import (P3-34)").
   - It makes each node_test's data its own import closure, so a source edit reruns only the tests that import it
     (`DC:206-213`).
4. **Inputs, declared** (aq, 405 inputs): 87 src and 114 test `.ts` files, both as sources and as their bin copies;
   `test-imports.mts`; the tool launcher and runfiles (Node). No npm package files.
5. **Inputs, actually read.**
   - Every file passed on argv (`$(rootpaths :import_scan)`, `test-imports.mts:14`), read with `readFileSync` (`:22`).
     It writes `argv[0]`. No environment variables.
   - Over-declared: none in effect. Each file in import_scan is either scanned or is a test.
   - Under-declared: none (the throw at `:30` guards it).
6. **Outputs.** `datacube/test_imports_generated.bzl`: `TEST_IMPORTS = {test name: [src files]}`.
7. **Committed?** Yes.
   - The file is `datacube/test_imports.bzl` (50,983 bytes, 1,893 lines).
   - Writer: `//datacube:update_generated` (`DC:428`). Diff test: `//datacube:update_generated_3_test`.
   - Suites: `//:generated` and `//:update_generated`.
8. **Who consumes it.**
   - **The build graph at load time:** `DC:21` loads it, and `DC:213` uses it to compute each node_test's `data`.
   - Not product. Not read at run time by anything.
   - **Humans:** `DC:206-207` says a test not yet in the list takes all of `:src` until `//datacube:update_generated`
     adds it.
9. **Determinism.** Yes. Tests are sorted (`:36`), source lists are sorted (`:56`), and there is no time or randomness.
10. **Cost.** One Node process doing regex over 201 files. Trivial.
11. **What reruns it today.** Any `.ts` edit under `datacube/src` or `datacube/test`; the tool; Node.
    - `q 'somepath(//datacube:test_imports, //datacube:test/fake-engine.ts)'` gives
      `test_imports → import_scan → test/fake-engine.ts`. That is a real input.
12. **What SHOULD rerun it.** Exactly that: the import lines of DataCube's src and test TypeScript.
    - It reruns on any content edit, not only on import edits. The action is cheap, and the output is byte-identical
      unless an import moved.
13. **Who runs it today.**
    - `bazel build //...`; `//gates:local` (`//:generated → update_generated_3_test`).
    - The `checks` lane, and the bump (needlessly: it has no upstream input).
14. **Recommendation: COMMITTED-SOURCE.**
    - Manual: yes.
    - Writer: `//:update_generated` (C/D).
    - Diff test: the everyday gate.
    - It is already narrow. Design §4.6: port it off Node.
15. **Open questions.** None.

## 6. //datacube:link_dictionary_next (and :cut_link_dictionary)

1. **Identity.**
   - A `js_run_binary` at `DC:309-314`, args `[_NEXT_LINK_VERSION]` (`"p2"`, `DC:128`), stdout `link-p2.gen.ts`.
   - Tool: `:make_link_dictionary` (`DC:292-303`), data `[":src", "tools/link-dictionary/make.ts"]`.
   - Program: `datacube/tools/link-dictionary/make.ts`; `makeDictionary` is at `:110-112` and output at `:114-123`.
   - Writer: `:cut_link_dictionary`, a `write_source_files` with `diff_test = False` and
     `check_that_out_file_exists = False`, writing `src/share/link-p2.ts` (`DC:316-321`).
2. **What it computes.**
   - The share link's next deflate preset dictionary, built from the product's live vocabulary:
     - enum words (`OPERATORS`; the aggregates and types from `OFFER_FACTS`; `CHART_MARKS`);
     - protocol words (`CALC_FUNCTIONS`, `WINDOW_FUNCTIONS`, node shapes);
     - a template page written by the product's own writers (`writeCube`, `writePage`, `pageToJson`), `make.ts:44-112`.
   - Its output is a TS module exporting `LINK_DICTIONARY_P2`.
3. **Why it exists.**
   - The tool came with `e9c15b6bb` (2026-09-28, share link, milestone 1b). The build target came with `ba6aa1959`
     (2026-10-04, "P2-08 (amended): the next link dictionary is a build output; a version is cut by bazel run").
   - **Why it is built by `//...` today:** "built with //... so the tool cannot rot unseen" (`DC:305`; `ba6aa1959`
     message).
   - The vocabulary grows by design: p1 predates the treemap mark. So a diff test of the tool against `link-p1.ts`
     would be red from the start. The freeze is p1's hash pin instead.
4. **Inputs, declared** (aq, 252 inputs): the same `:src` closure and 122 npm files as §3, plus `make.ts`.
5. **Inputs, actually read.**
   - The import closure of `make.ts` is 48 files:
     - 25 in `datacube/src`, including `generated/offer-facts.ts` and `generated/catalog-facts.ts`;
     - 11 in `engine-client/src`, including `generated/lite-facts.ts`;
     - 10 in `pure-protocol/src`;
     - 2 in `query-store/src`.
   - It has no bare imports and writes stdout.
   - **Over-declared:** about 62 datacube sources, 4 query-store sources, and all 122 npm files.
   - **Real dependency on a committed generated file:** `OFFER_FACTS` (§4) shapes the dictionary.
6. **Outputs.** `datacube/link-p2.gen.ts`: the frozen-dictionary module, version p2, with a "FROZEN" header.
7. **Committed?**
   - No diff-tested copy. `cut_link_dictionary` writes `src/share/link-p2.ts` once, by hand, as a draft.
   - Today only `src/share/link-p1.ts` exists (`ls datacube/src/share`).
   - Neither writer is in `//:update_generated`.
8. **Who consumes it.**
   - Only `:cut_link_dictionary` (rdeps1). No test and no product code.
   - The shipped dictionary is the committed `link-p1.ts`: `src/share/link.ts:20` imports it, and it is in the closure
     of `demo/main.ts` and `demo/page.ts`.
   - **The versioning flow:**
     1. `bazel run //datacube:cut_link_dictionary` writes `src/share/link-p2.ts`.
     2. `share_link_test` (`datacube/test/share-link.test.ts:89-111`) then fails until the person pins its sha256 in
        `PINNED` and moves `_NEXT_LINK_VERSION` on in `DC:128`.
     3. `DC:136-139` hands the test `LINK_VERSIONS` (a glob of `src/share/link-*.ts`) and `NEXT_LINK_VERSION`.
     4. A version is frozen by its hash, never by rerunning the tool (`DC:288-291`).
   - The guard moved from a load-time failure (`ba6aa1959`) to this test in `053e15006` (2026-10-04).
   - **Humans:** `DC:305-308`, `docs/GATES.md:64`, `docs/DATACUBE_SAVE_SHARE_2026_09_28.md:254`, `make.ts:11-12`.
9. **Determinism.** Yes. Literal tables, `Object.keys` of literal objects, and `JSON.stringify` (`make.ts:122`).
10. **Cost.** One Node process. Trivial.
11. **What reruns it today.**
    - Any edit to `:src`'s 117 TS files or the npm locks.
    - `q 'somepath(//datacube:link_dictionary_next, //datacube:src/generated/offer-facts.ts)'` gives
      `link_dictionary_next → make_link_dictionary → //datacube:src → src/generated/offer-facts.ts`. So every core
      change that regenerates `offer-facts.ts` reruns it.
12. **What SHOULD rerun it.** A human cutting a version.
    - The "rot" it guards against is mostly caught already. `//datacube:typecheck_test` type-checks `tools/**/*.ts`
      (`DC:245-269`), and `make.ts`'s `Required<ColumnFormat>`/`Required<ColumnConfiguration>` (`:27,34`) turn a new
      field into a type error there.
    - What typecheck cannot catch is a runtime throw in the writers.
13. **Who runs it today.**
    - Only `bazel build //...` (the CI `build` lane on every platform). `//gates:local` does not reach it
      (`q 'somepath(//gates:local, //datacube:link_dictionary_next)'` is empty).
    - The bump's `bazel test //...` does not build non-test targets either, except through tests.
    - By hand: `cut_link_dictionary`.
14. **Recommendation: DRAFT-MANUAL.**
    - Manual: yes.
    - Writer: `cut_link_dictionary` stays out of every update group (by hand, once per version).
    - No diff test. The hash pin in `share_link_test` is the freeze.
    - If runtime rot matters, a small test that imports `makeDictionary()` and checks that it returns (a test, not a
      `//...` side effect) replaces the build-time run.
    - Narrowing: the 48-file closure, no npm.
15. **Open questions.** None.

## 7. //datacube:cube_queries

1. **Identity.**
   - A `js_run_binary` at `DC:446-461`: srcs `test/wasm-differential/cases.ts` and `emit.ts`; outputs
     `cube_model.pure` and `cube_queries.tsv`.
   - Tool: `:emit_cube_queries` (`DC:436-444`), data `[":src"]`.
   - Program: `datacube/test/wasm-differential/emit.ts:12-21`.
2. **What it computes.** The trades model and one `name<TAB>lambda-JSON` line per DataCube cube case (`cases.ts`
   `queries()`, 50 named cases), serialized by the product's query code, for the WASM differential.
3. **Why it exists.**
   - Introduced by `b02032e69` (2026-09-23, "DataCube and the planner in WebAssembly, in main under Bazel"), and moved to
     protocol JSON in `149e771b4` (T4b).
   - It feeds `cube_jvm_answers` and so `wasm_differential_test`, which checks that the queries DataCube SENDS plan
     identically in the browser module and on the JVM (`datacube/test/wasm-differential/compare.ts:1-15`).
4. **Inputs, declared** (aq, 254 inputs): as §3 (87, 14, 10 and 6 sources, 122 npm files, the tool), plus `cases.ts`
   and `emit.ts`.
5. **Inputs, actually read.**
   - The closure of `emit.ts` is 36 files: 15 in `datacube/src` (including `adhoc/query.ts`, `adhoc/state.ts`,
     `generated/offer-facts.ts`), `cases.ts`, 10 in `engine-client/src`, and 10 in `pure-protocol/src`.
   - Bare imports: `node:fs` only.
   - **Over-declared:** about 72 datacube sources, 4 engine-client sources, query-store, and all npm files.
6. **Outputs.** `datacube/cube_model.pure` and `datacube/cube_queries.tsv`.
7. **Committed?** No. A build output consumed by `cube_jvm_answers`.
8. **Who consumes it.** `//datacube:cube_jvm_answers` only (rdeps1). `wasm_differential_test` re-derives the same queries
   from `cases.ts` itself (`compare.ts:25`).
9. **Determinism.** Yes. Fixed cases, compact JSON, and no time.
10. **Cost.** One Node process. Trivial.
11. **What reruns it today.**
    - Any `:src` edit and the npm locks.
    - `q 'somepath(//datacube:cube_queries, //datacube:src/export-xlsx.ts)'` gives
      `cube_queries → emit_cube_queries → //datacube:src → export-xlsx.ts`, a file it never imports.
12. **What SHOULD rerun it.** `cases.ts`, `emit.ts` and their 34-file product closure (the cube's query serializer).
13. **Who runs it today.**
    - `bazel build //...`; `//gates:local` (through `//datacube:tests → wasm_differential_test → cube_jvm_answers`).
    - The CI `app` lane; the bump's `test //...`.
14. **Recommendation: BUILD-OUTPUT, testonly.** It exists only for `wasm_differential_test`.
    - Manual: yes.
    - No writer.
    - Narrowing: the 36-file closure, no npm.
15. **Open questions.** None.

## 8. //datacube:cube_jvm_answers

1. **Identity.**
   - A `java_run` at `DC:463-479`, args `cube_model.pure cube_queries.tsv trades::RT {OUT} json`.
   - Program: `planner.JvmMain` (`wasm/src/main/java/planner/JvmMain.java:32-55`), deps `//wasm:jvm_main`.
2. **What it computes.** For each line of `cube_queries.tsv`, JvmMain calls `Wasm.planJsonOrError(model, lambdaJson,
   runtime)` (`wasm/src/main/java/planner/Wasm.java:166-177`). That is `Compiler.query(Compiler.compileModel(model),
   ProtocolReader.lambda(...)).plan(runtime)`, giving `{sql, type}` (`UpstreamRelationType.of`) or the folded error. It
   writes `<<<name>>>\n<answer>\n<<<END>>>\n` blocks.
3. **Why it exists.** `b02032e69` (2026-09-23). It is the JVM half of DataCube's WASM differential: "the JVM answers them
   (a build output)" (`DC:433-435`).
4. **Inputs, declared** (aq, 148 inputs).
   - 28 jars: 24 core (the plan side: builtin, cache, compiler, compiler_element_type, database, diagnostics, error,
     execution_plan, lexer, lineage, lowering, model, normalizer, parser, plan, planner, platform, protocol, resolver,
     spi, sql, sql_dialect, validation, values), base, json, `wasm/boundary`, `wasm/jvm_main`.
   - The 2 `cube_queries` outputs and 118 JDK files.
   - `teavm_api` is `neverlink` (`W:22-29`), so no TeaVM jar is in the action.
5. **Inputs, actually read.**
   - Classes used: `Compiler` (`//core:planner`), `QueryPlan` and `UpstreamRelationType` (`//core:plan`),
     `ProtocolReader` (`//core:protocol`), `Json` (`//json`). The minimum runtime closure is the 24-library planner set.
   - It reads the two argv files and writes `args[3]`.
   - **Over-declared:** `diagnostics` and `execution_plan`, which `//core:plan_side` adds over `//core:planner`
     (`core/BUILD.bazel:220-224`).
6. **Outputs.** `datacube/cube_jvm_answers.txt`.
7. **Committed?** No. A build output read by `//datacube:wasm_differential_test`, with
   `CUBE_JVM_ANSWERS=$(rlocationpath :cube_jvm_answers)` (`DC:481-493`, `compare.ts:31`).
8. **Who consumes it.** `wasm_differential_test` only. It is not product, and no doc tells a human to run it
   (`docs/WAREHOUSE_D1_DESIGN_2026_09_26.md:28` only cites it).
9. **Determinism.** Yes. A `LinkedHashMap` in TSV order (`JvmMain.java:34`). Answers are SQL plus a JSON type, or an
   exception class and message.
10. **Cost.** One JVM: 50 model compiles and plans. **No DuckDB:** the plan side carries no driver
    (`core/BUILD.bazel:217-219`), and no database is started.
11. **What reruns it today.**
    - Every plan-side core jar (`Compiler.java` and the rest); changes to `cube_queries`' bytes; `Wasm.java`,
      `JvmMain.java`; the JDK.
    - `q 'somepath(//datacube:cube_jvm_answers, //datacube:src/catalog-model.ts)'` shows the JS side, through
      `cube_queries → emit_cube_queries → :src`. The action reruns only if the TSV bytes change.
12. **What SHOULD rerun it.** Engine behaviour (the planner closure) and the cube cases or serializer. It is a test
    oracle.
13. **Who runs it today.**
    - `bazel build //...`; `//gates:local` (`//datacube:tests → wasm_differential_test`).
    - The CI `app` lane; the bump's `test //...`.
14. **Recommendation: BUILD-OUTPUT, testonly** (design group E).
    - Manual: yes; only its test reaches it.
    - No writer. Its test stays in `//datacube:tests`.
    - Narrowing: keep `//wasm:jvm_main → :boundary → //core:plan_side`. The 2 extra jars are the price of one boundary
      class for both targets (`W:31-49`).
15. **Open questions.** None.

## 9. //datacube:dist

1. **Identity.**
   - A `js_run_binary` at `DC:759-768`, srcs `[":site"]`, args `datacube datacube/dist`, `out_dirs = ["dist"]`.
   - Tool: `:make_dist` (`DC:754-757`), a Node js_binary.
   - Program: `datacube/demo/make-dist.mjs:17-55`.
2. **What it computes.** A flat static-host folder of the DataCube page:
   - it copies `bundle.js`, `planner-worker.js`, `trades.pure` and `chunks-bundle/` (`:29-33`), and 6 runtime files into
     `vendor/` (`:34-38`);
   - it writes `index.html` with its `../src/*.css` stylesheets inlined, because a flat folder has no `../src`
     (`:40-50`).
3. **Why it exists.**
   - `b02032e69` (2026-09-23): "a DataCube deployment is a directory of files" (`make-dist.mjs:1-9`).
   - Today it is also the `--site` of `//datacube:app` (`DC:741-750`), the single-user native app, which serves it with
     `/config.json` synthesized by the warehouse (`warehouse/src/main/java/com/legend/warehouse/server/WarehouseServer.java:593-604`).
4. **Inputs, declared** (aq, 38 inputs).
   - Everything in `:site` (`DC:710-721`): the 5 bundles and their 4 chunk dirs (including `remote-bundle` and
     `stress`), the 4 html files, `config.json`, `fonts.css`, 3 `.pure` files, 3 css files, `vendor/` (from
     `//wasm:planner`, duckdb-wasm and `//legend-art:fonts`), `projects/trading`.
   - The tool and its runfiles (Node).
5. **Inputs, actually read.**
   - It reads `demo/{bundle.js, planner-worker.js, trades.pure, index.html}`, `demo/chunks-bundle/`, 6 named files in
     `demo/vendor/`, and the three css files `index.html` links (`theme.css`, `grid/grid.css`, `app.css`).
   - **Over-declared:** `bundle-page.js` and `chunks-bundle-page`, `remote-bundle.js` and its chunks, `stress.js` and its
     chunks, `page.html`, `remote.html`, `stress.html`, `config.json`, `fonts.css`, `torture.pure`, `trades-h2.pure`,
     `projects/`, and `vendor/fonts/`.
   - **Product gap:** the declared-but-not-copied list includes files the page needs.
     - `index.html:11` links `fonts.css`, which `dist/` lacks, and `vendor/fonts` is not copied either. So `//datacube:app`
       serves the page without its fonts.
     - `config.json` is not copied. That is harmless for the app, whose warehouse answers `/config.json`. For a static
       host, `datacube/demo/page-config.ts:72-90` treats a missing file as empty.
     - Confirmed on disk: `ls -la bazel-bin/datacube/dist` (built 2026-10-05 16:23) holds only `bundle.js`,
       `chunks-bundle/`, `index.html`, `planner-worker.js`, `trades.pure` and `vendor/` with 6 files.
     - The fix exists on branch `bazel/exec` as `dab833263` (P4-11's completeness half, with `dist_complete_test`). It is
       not on build/rebuild.
6. **Outputs.** The tree `datacube/dist/`.
   - **The files are absolute symlinks into the output base.** `fs.promises.cp` copies the sandbox's input symlinks as
     symlinks, for example `bundle.js -> (local path)`
     (on disk, same listing).
   - Only `index.html` is a real file. This is audit finding P2-349
     (`docs/DATACUBE_AUDIT_PASS2_FINDINGS_2026_09_26.md:345`), still true.
7. **Committed?** No. A build output consumed by:
   - `//datacube:app` (its `_posix_launcher` and `launcher_binary` pass it as `--site`, `warehouse/defs.bzl:60-62,151-153`);
   - `//datacube:verify_app` (manual, `DC:861-874`);
   - `//datacube:verify_app_test` (`DC:880-897`, which reads `join(DATACUBE, 'dist')` in `datacube/demo/verify-app.mjs:87`).
8. **Who consumes it.**
   - **Product:** the `//datacube:app` launcher (shipped through `bazel run`, `docs/DATACUBE_ON_POSTGRES.md`).
   - **Tests:** `verify_app_test`. The public two-app site does **not** use it: `//site:dist` copies `//datacube:site`
     whole (`site/BUILD.bazel:12-22`).
   - **Humans:** `datacube/demo/README-realdata.md:120` ("bazel build //datacube:dist"), `make-dist.mjs:3`.
9. **Determinism.**
   - **No, in the material sense.** The symlink targets name the builder's output base, so the folder depends on the
     machine and the output-base path.
   - Bazel digests tree contents through the links, but a remote or disk-cache hit materializes real files. So the
     folder's form differs between a local run and a cache hit.
   - The bundle bytes inside also carry build paths (design §5c, D15).
10. **Cost.** One Node process copying files. It depends on the full site: 5 esbuild bundles, the TeaVM planner compile (`//wasm:planner`), and duckdb-wasm.
11. **What reruns it today.**
    - Anything in `:site`'s closure: every DataCube, engine-client, pure-protocol and query-store source; every demo
      `.ts` (each bundle globs `demo/**/*.ts`, `DC:634`); the planner's whole Java plan side; the fonts; duckdb-wasm.
    - `q 'somepath(//datacube:dist, //datacube:demo/remote-harness.ts)'` gives
      `dist → site → bundle_bundle → bundle_bundle_srcs → demo/remote-harness.ts`.
    - `q 'somepath(//datacube:dist, //wasm:src/main/java/planner/Wasm.java)'` gives
      `dist → site → vendor → //wasm:planner → //wasm:boundary → Wasm.java`.
12. **What SHOULD rerun it.** The shipped bundles (`//datacube:bundles`), the vendor runtimes, and the page's own
    html/css. It is packaging of compile outputs.
13. **Who runs it today.**
    - `bazel build //...`; `//gates:local` (through `//datacube:verify_app_test`).
    - The CI `app` lane (`verify_app_test`) and `native` lane (`//datacube:app` on Linux, macOS, Windows and Linux
      arm64).
    - The bump's `test //...`.
    - By hand: `bazel run //datacube:app`.
14. **Recommendation: BUILD-OUTPUT (packaging), to be replaced in the D10 step.**
    - **Role in D10 ("one shape per app").** It is DataCube's second packaging path, the per-app script D10 removes:
      `//<app>:site` becomes one shared Bazel rule, `//site:dist` combines the apps, and `make-dist.mjs` goes.
    - The constraint that kept it alive (`dab833263`'s message): the warehouse serves `--site` as its root, while
      `//datacube:site`'s layout is `demo/` beside `src/`. So `index.html`'s `../src/*.css` links need either the
      inlining step (esbuild CSS bundling, per P4-11) or a layout the shared rule produces.
    - The shared rule must also copy real files, never symlinks, and carry `fonts.css` and `vendor/fonts`. The D5
      removals (remote and stress bundles, `torture.pure`) apply to the same site.
    - Not testonly; it is the app's site. Manual: no once it is part of `//:sites` (packaging tier). Until then it should
      stay out of the compile tiers.
    - No writer and no diff test. A `dist_complete_test`-style check belongs with the shared rule.
15. **Open questions.**
    1. Will `//datacube:app` serve `//site:dist`'s `/datacube/` subtree, or its own D10 site? Settle in the D10 design.
       The warehouse's site root and its `/config.json` behaviour (`WarehouseServer.java:593-604`) are the constraint.
    2. Should `dab833263` (bazel/exec) be merged first as the stop-gap? The user decides.

## 10. //engine-client:lite_facts

1. **Identity.**
   - A `java_run` at `EC:47-53`, args `{OUT}`.
   - Program: `typefacts.TypeFacts` (`engine-client/tools/typefacts/TypeFacts.java:34-83`).
   - Library `:type_facts_main` (`EC:39-45`), deps `//core`.
2. **What it computes.** Every type name either compiler reports for a column, mapped to its plain primitive and its
   family:
   - legend-lite's primitives by leaf and path, and legend-engine's precise primitives by FQN (`Type.Primitive.values()`,
     `byFqn()`);
   - `Variant` and `Any` (`PlatformTypes.VARIANT/ANY`);
   - plus `Type.RelationType.PIVOT_SEPARATOR`.
3. **Why it exists.**
   - `17b8f9034` (2026-09-27, "DataCube types come from the compiler…", decision D1 of
     `docs/DATACUBE_TYPES_TO_SERVER_2026_09_27.md`).
   - Moved to engine-client in `be3fc8d7d` (2026-10-04).
   - **Stale doc:** `TypeFacts.java:26-27` still says "Built by //datacube:type_facts … bazel run //datacube:update_generated".
4. **Inputs, declared** (aq, 152 inputs): 34 jars (31 core, base, json, type_facts_main) and 118 JDK files.
5. **Inputs, actually read.**
   - `Type` and `PlatformTypes`, both in `//core:compiler_element_type`. Its closure is 13 libraries: base, builtin,
     compiler_element_type, error, lexer, model, parser, platform, protocol, spi, sql, values, json.
   - `Type.java` imports the generated `com.legend.builtin.Pure` (`C/compiler/element/type/Type.java:3`).
   - It writes `args[0]`.
   - **Over-declared:** 20 core jars: cache, compiler, database, diagnostics, driver, exec, execution_plan, ide,
     lineage, lowering, normalizer, plan, planner, probe, resolver, server_lib, sql_dialect, test, testdatagen,
     validation.
6. **Outputs.** `engine-client/generated/lite-facts.gen.ts`: `TypeFamily`, `TypeFact`, `TYPE_FACTS` and `PIVOT_SEPARATOR`.
7. **Committed?** Yes.
   - The file is `engine-client/src/generated/lite-facts.ts` (4,139 bytes).
   - Writer: `//engine-client:update_generated` (`EC:55-62`). Diff test: `//engine-client:update_generated_test`.
   - Suites: `//:generated` (`ROOT:86`) and `//:update_generated` (`ROOT:113`).
8. **Who consumes it.**
   - **Product at run time:**
     - `engine-client/src/types.ts:11` (`TYPE_FACTS`);
     - `PIVOT_SEPARATOR` in `datacube/src/snapshot.ts:16`, `calc.ts:29`, `query.ts:40`, `adhoc/query.ts:15` and
       `grid/columns.ts:35`;
     - it is in the closure of DataCube's two app bundles and Query's bundle (not Studio's).
   - **Tests:** `datacube/test/calc.test.ts:20`, `infer.test.ts:2`, and the tests reaching it through `test_imports.bzl`.
   - **Harness:** `datacube/demo/grid-invariants.mjs:17`.
   - **Generators:** in the closures of `offer_queries`, `cube_queries` and `link_dictionary_next`.
   - **Humans:** `engine-client/README.md:14`, `EC:57`.
   - `docs/GATES.md:48` still calls it "DataCube's lite-facts.ts".
9. **Determinism.** Yes (`TreeMap`, `:38`). One copy passes on 4 platforms.
10. **Cost.** One JVM, a few enum walks. Trivial. No database.
11. **What reruns it today.**
    - Every core jar.
    - `q 'somepath(//engine-client:lite_facts, //core:src/main/java/com/legend/Compiler.java)'` gives
      `lite_facts → type_facts_main → //core:core → //core:planner → Compiler.java`, a class it never loads.
12. **What SHOULD rerun it.**
    - `C/compiler/element/type/*.java` (`Type`, `PlatformTypes`) and what they read: the 13-library closure, which
      includes the generated `Pure.java`. So an upstream bump can move it.
    - `TypeFacts.java`.
13. **Who runs it today.**
    - `bazel build //...`; `//gates:local` (`//:generated → //engine-client:update_generated_test`).
    - The `checks` lane, and the bump.
    - By hand: `bazel run //engine-client:update_generated` (`EC:38,57`).
14. **Recommendation: COMMITTED-SOURCE.**
    - Manual: yes.
    - Writer: `//:update_generated`.
    - Diff test: the everyday gate.
    - Narrowing: `type_facts_main` deps `["//core:compiler_element_type"]` (closure 13, 11 core jars).
15. **Open questions.** None.

## 11. //legend-art:icons_gen

1. **Identity.**
   - A `js_run_binary` at `LA:103-109`, srcs `[":react_icons"]`, args `$(rootpath :react_icons)`, stdout
     `icons.gen.ts`.
   - Tool: `:icons_tool` (`LA:77-80`), program `legend-art/tools/icons.mjs` (work at `:12-13,133-163`).
   - `:react_icons` (`LA:97-101`) copies the 10 `@react_icons//:<set>/index.mjs` files listed in `_ICON_SETS`
     (`LA:84-95`).
2. **What it computes.**
   - For each entry of its own `ICONS` table (our name → react-icons set, react-icons name, upstream Legend's name;
     `icons.mjs:16-131`), it regex-extracts the `GenIcon({...})` JSON from that set's `index.mjs` (`:137-147`).
   - It renders SVG markup with GenIcon's defaults and writes `export const ICONS = {...} as const; export type
     IconName` (`:149-163`).
3. **Why it exists.**
   - Born as `//query:icons_gen` in `e35e32871` (2026-10-04, P2-07, "query/src/ui/icons.ts is generated by Bazel from a
     pinned react-icons").
   - **Moved to legend-art** in `80ee5faa1` (2026-10-04, "legend-art: icons generated from the pinned @react_icons…").
     That commit is in PR #24's merge `06a290b76` (`git merge-base --is-ancestor 80ee5faa1 06a290b76` holds).
   - Today it is the one icon generator for Query and Studio (`LA:74-76`). `query/BUILD.bazel` has no icons target
     (`grep icons query/BUILD.bazel` finds only the comment at `:29`).
4. **Inputs, declared** (aq, 4 inputs): the `react_icons` tree (10 files), the bin copy of `icons.mjs`, and the tool
   launcher and runfiles (Node).
5. **Inputs, actually read.**
   - It reads `<root>/<set>/index.mjs` only for the sets its table names (`:141-143`); all 10 are used. It writes
     stdout.
   - The react-icons version is **hard-coded** in the header string (`icons.mjs:151`, "react-icons 5.5.0"). So bumping
     `@react_icons` (`MODULE.bazel:322-327`) also needs an edit to `icons.mjs`.
   - Over- and under-declared: none.
6. **Outputs.** `legend-art/icons.gen.ts`.
7. **Committed?** Yes.
   - The file is `legend-art/src/icons.ts` (62,720 bytes).
   - Writer: `//legend-art:update_generated` (`LA:111-116`). Diff test: `//legend-art:update_generated_test`.
   - Suites: `//:generated` (`ROOT:90`) and `//:update_generated` (`ROOT:117`).
8. **Who consumes it.**
   - **Product at run time:**
     - Query: `query/src/ui/dom.ts:5` imports `ICONS`, and the file is in `query/demo/main.ts`'s closure.
     - Studio: through `legend-art/src/icon.ts:4`, which `studio/src/ui/dom.ts:3`, `editor.ts:13`, `sdlc-panels.ts:9` and
       `selector.ts:5` import; it is in `studio/demo/main.ts`'s closure. `legend-art/src/type-icon.ts:6` imports its
       types.
     - Through `legend-art:legend_art`, visible to `//query` and `//studio` (`LA:9-16`).
   - **Humans:** `legend-art/README.md:13-20`, `LA:113`.
   - **Stale references:** `MODULE.bazel:320` ("query/src/ui/icons.ts … (//query:icons_gen)"), `docs/GATES.md:46,49`
     ("Query's icons.ts", `//query:update_generated_test`), and `studio/docs/UPSTREAM_STUDIO_LOOK.md:1215`.
9. **Determinism.** Yes. Object insertion order, a pinned archive, and no time.
10. **Cost.** One Node process doing regex over 10 files. Trivial.
11. **What reruns it today.** `icons.mjs`, the `@react_icons` pin, `_ICON_SETS`, and Node or rules_js. That is already
    its true closure.
12. **What SHOULD rerun it.**
    - **Mainly our file:** an edit to the `ICONS` table in `icons.mjs` (an app needs a new icon), and `_ICON_SETS`.
    - **Rarely:** a `@react_icons` pin bump. It follows legend-studio's version, not the engine release, and
      `tools/bump/Bump.java` does not touch it.
13. **Who runs it today.**
    - `bazel build //...`; `//gates:local` (`//:generated → //legend-art:update_generated_test`).
    - The `checks` lane; the bump (`//:update_generated`), although no bump input reaches it.
    - By hand: `bazel run //legend-art:update_generated`.
14. **Recommendation: COMMITTED-SOURCE.**
    - Its common trigger is our own `icons.mjs` table. The design's group A ("upstream bump only",
      `BUILD_REBUILD_DESIGN_2026_10_05.md:183`) misses that, and the bump never moves `@react_icons`.
    - Manual: yes.
    - Writer: `//:update_generated` (C/D). Diff test: the everyday gate (4 inputs, no JVM).
    - No narrowing needed. Fix the stale references above.
15. **Open questions.** None.

## 12. //warehouse:duckdb_library

1. **Identity.**
   - A `jar_entry` at `WH:263-268`: entry `_DUCKDB_LIBRARY`, chosen per platform (`WH:19-25`, for example
     `libduckdb_java.so_osx_universal`); jar `@duckdb_jdbc_warehouse//jar`; `target_compatible_with` the 5 platforms.
   - Rule: `warehouse/defs.bzl:17-44`, which runs `@bazel_tools//tools/zip:zipper x <jar> -d <dir> <entry>`.
2. **What it computes.** It takes DuckDB's native library for this platform out of DuckDB's JDBC jar as an ordinary
   output file. No script unzips it at run time.
3. **Why it exists.** Bazel workplan P1-16 (`WH:261-262`, `WH:68-70`). The warehouse calls DuckDB's C API through
   `java.lang.foreign` and uses no JDBC class. Its native library must be a declared file the server finds:
   `--duckdb-library`, or beside the native image, or from runfiles (`ServerRunfiles`).
4. **Inputs, declared** (aq, 2 inputs): `external/+product_jars+duckdb_jdbc_warehouse/jar/duckdb_jdbc-1.5.5.1.jar` and
   the prebuilt zipper (`external/bazel_tools/tools/zip/zipper/zipper`).
   - **After the http_jar change** (`0eb4e6b88`, 2026-10-05, `tools/deps/jars.bzl:1-94`), `@duckdb_jdbc_warehouse//jar`
     is an `http_jar` java_import of the Maven Central jar, checked by sha256 (`jars.bzl:33-37,80-89`).
     - `jar_entry` reads `JavaInfo.runtime_output_jars` (`defs.bzl:18`), which is the downloaded jar unchanged.
     - Before, it read `@maven_warehouse//:org_duckdb_duckdb_jdbc`, the rules_jvm_external artifact, which was stamped
       and copied (`git show 0eb4e6b88 -- warehouse/BUILD.bazel`; `jars.bzl:8-12` gives 17.4 s of a 19.7 s critical
       path for that).
5. **Inputs, actually read.** The one entry of that jar. Nothing else.
6. **Outputs.** `warehouse/libduckdb_java.so_<platform>` (108,682,352 bytes on macOS, `ls -la bazel-bin/warehouse/`).
   The name must not change: `WH:262`, "DO NOT RENAME: the server finds this output in its runfiles by its path".
7. **Committed?** No. A build output: `data` of `//warehouse:server` (`WH:83-86`), and a file of every warehouse
   launcher and test.
8. **Who consumes it** (rdeps1).
   - **Product:** `//warehouse:server` (data), and the launchers `serve_posix/windows` and `datacube:app_posix/windows`.
   - **Tests:** `//warehouse:tests` and `tests_native` (`-Dwarehouse.duckdb.library=$(rlocationpath …)`, `WH:148`);
     `postgres_live` and `postgres_live_native`; `launcher_test_serve_site_*`; `//datacube:live_snap_test`
     (`WAREHOUSE_DUCKDB_LIBRARY`, `DC:602-608`); `//query:verify`; `//spec:judge_host_warehouse` and
     `judge_database_warehouse`.
9. **Determinism.** Yes. A byte-for-byte extraction of a checksummed archive member.
10. **Cost.** One zipper extraction (about 109 MB written). No JVM or database.
11. **What reruns it today.** The `duckdb_jdbc_warehouse` entry in `tools/deps/jars.bzl` (coordinate or sha256), the
    platform choice, or Bazel's zipper.
12. **What SHOULD rerun it.** The same, which is already right. It moves with the DuckDB 1.5.x pin, and the postgres
    extension pin must move with it (`MODULE.bazel:358-360`, "it must match the library's version exactly").
13. **Who runs it today.**
    - `bazel build //...`; `//gates:local` (`//warehouse:tests`).
    - CI `app` (`//warehouse:tests`), `native`, and `browser` (`live_snap_test`).
    - The bump's `test //...`.
14. **Recommendation: BUILD-OUTPUT** (product data; not testonly).
    - Manual: no; it is product data of the server.
    - No writer. Already narrow.
    - D9 keeps it as a declared `data` dependency handed to the server by `$(rootpath)`.
15. **Open questions.** None.

## 13. //warehouse:duckdb_extensions

1. **Identity.**
   - A `java_run` at `WH:370-382`, mnemonic `GunzipDuckdbExtension`, srcs `[":duckdb_extension_gz"]` (an alias of
     `POSTGRES_EXTENSION`, `WH:363-367`).
   - Program: `com.legend.tools.gunzip.Gunzip` (`tools/gunzip/Gunzip.java:16-25`), deps `//tools/gunzip`.
2. **What it computes.** It gunzips DuckDB's postgres extension for this platform (the pinned
   `postgres_scanner.duckdb_extension.gz`) on the JDK alone.
3. **Why it exists.**
   - Bazel workplan P1-17 (`WH:358-362`). Bazel 9.2's own `.gz` unpacking sets a modification time 1000x in the future,
     which Windows refuses (CI run 37224856941, 2026-10-04).
   - It replaced a `run_shell` with host `gzip`. The output must not be renamed (`WH:369`).
4. **Inputs, declared** (aq, 120 inputs).
   - `tools/gunzip/libgunzip.jar`.
   - `external/+http_file+duckdb_postgres_extension_<platform>/file/downloaded`, the http_file pinned per platform by
     integrity at `https://extensions.duckdb.org/v1.5.5/<platform>/postgres_scanner.duckdb_extension.gz`
     (`MODULE.bazel:358-372`, `warehouse/defs.bzl:48-54`).
   - 118 JDK files.
   - **The http_jar change does not touch it.** It reads an `http_file`, not a jar.
5. **Inputs, actually read.** `args[0]` (the `.gz`); it writes `args[1]`. Nothing else. Over- and under-declared: none.
6. **Outputs.** `warehouse/duckdb_extensions/postgres_scanner.duckdb_extension`, plus the tmp dir.
7. **Committed?** No. A build output: `data` of `//warehouse:server` (`WH:83-85`), and copied into the launchers'
   extension directories (`serve_extensions`, `datacube:app_extensions`, `launcher_test_serve_site_extensions`; rdeps1).
8. **Who consumes it.**
   - **Product:** `//warehouse:server`, `//warehouse:serve`, `//datacube:app`.
   - **Tests:** `postgres_live` and `postgres_live_native` (`-Dwarehouse.postgres.extension=$(rlocationpath …)`,
     `WH:231`); `launcher_test` (through `:serve`); `datacube:verify_app_test` (through `//warehouse:serve`,
     `q 'somepath(//datacube:verify_app_test, //warehouse:duckdb_extensions)'`).
9. **Determinism.** Yes. A gunzip of a checksummed file.
10. **Cost.** A tiny JVM. No database.
11. **What reruns it today.** The `duckdb_postgres_extension_*` pins, `Gunzip.java`, the JDK, and `java_run`. That is
    already its true closure.
12. **What SHOULD rerun it.** The same: the extension pin, which follows the DuckDB library version.
13. **Who runs it today.**
    - `bazel build //...`; `//gates:local` (through `//datacube:verify_app_test → //warehouse:serve`).
    - CI `app` (`verify_app_test`) and `native` (`launcher_test`, `//datacube:app`); the bump's `test //...`.
14. **Recommendation: BUILD-OUTPUT** (product data; not testonly).
    - Manual: no.
    - No writer. Already narrow.
    - The design records that, as a `java_run`, it cannot sit in `//:native` without failing the compile-only guard
      (`BUILD_REBUILD_DESIGN_2026_10_05.md:360`). Once a Bazel release fixes the `.gz` mtime bug, an `http_file` with
      Bazel's own decompression (or `http_archive`) can replace the action, making it pure fetched data (memory:
      Bazel-native first; prove on Windows CI).
15. **Open questions.** Has a Bazel release since 9.2 fixed `GzFunction`'s mtime? Check the Bazel release notes, or
    retry on a throwaway Windows CI run.

## 14. //warehouse:reachability_metadata

1. **Identity.**
   - A `java_run` at `WH:388-396`, testonly, mnemonic `Generate`, args `{OUT}`.
   - Program: `com.legend.warehouse.server.duck.ReachabilityMetadata`
     (`warehouse/src/test/java/com/legend/warehouse/server/duck/ReachabilityMetadata.java:80-130`).
   - deps `[":tests_lib"]`.
2. **What it computes.** GraalVM's `reachability-metadata.json` for the native warehouse:
   - `foreign.downcalls`: every distinct signature in `Duck.DOWNCALLS` plus `Duck.RELEASE`, in GraalVM's notation;
   - `directUpcalls`: `AuthenticatedUser.UPCALLS`;
   - `reflection`: the upcall methods plus a declared `SERVICES` list of JCA providers and locale bundles, each with
     why (`:48-69`);
   - `resources`: a declared list of service files plus ICU's `nfc.nrm` (`:73-78`).
3. **Why it exists.**
   - `81a70fe8d` (2026-10-05, "The native image's reachability metadata is generated whole; no section recorded by hand
     (P2-09 (b), A23)").
   - The committed file dates from `0c28aa3b1` (2026-09-26, W1e), when it was recorded by GraalVM's tracing agent. An
     agent's output names the recording host's locale bundles and providers, so it cannot be one golden for three
     platforms (`:21-23`).
4. **Inputs, declared** (aq, 136 inputs).
   - 7 jars of ours: `tests_lib`, `client`, `sqlapi`, `server_lib`, `base`, `json`, `testing`.
   - 2 runfiles-library jars (`@rules_java//java/runfiles`).
   - 8 JUnit/opentest4j/apiguardian jars from `maven_test`.
   - The 77 MB `duckdb_jdbc-1.5.5.1.jar`.
   - 118 JDK files. 0 core jars.
5. **Inputs, actually read.**
   - `Duck` and `AuthenticatedUser`, both in `//warehouse:server_lib` (same package, package-private access), plus
     `java.lang.foreign` layouts. `Duck`'s static init builds descriptors and `Linker.nativeLinker()`
     (`Duck.java:38,88-206`) but **loads no library**: `load(Path)` is separate (`:74-80`).
   - It writes `args[0]`.
   - **Over-declared:** `tests_lib`'s other test classes (for example
     `q 'somepath(//warehouse:reachability_metadata, //warehouse:src/test/java/com/legend/warehouse/WarehouseServerTest.java)'`
     gives `reachability_metadata → tests_lib → WarehouseServerTest.java`), `client`, `testing`, the 8 JUnit jars, and the
     DuckDB jar.
   - **Self-loop:** `server_lib`'s resources are `glob(["src/main/resources/**"])` (`WH:74-75`), which includes the
     committed output `src/main/resources/META-INF/native-image/com.legend/warehouse/reachability-metadata.json`.
     `q 'somepath(//warehouse:reachability_metadata, //warehouse:src/main/resources/META-INF/native-image/com.legend/warehouse/reachability-metadata.json)'`
     gives `reachability_metadata → tests_lib → server_lib → reachability-metadata.json`. So writing the committed copy
     reruns the generator once more. It is idempotent.
6. **Outputs.** `warehouse/generated/reachability-metadata.json`.
7. **Committed?** Yes.
   - The file is `warehouse/src/main/resources/META-INF/native-image/com.legend/warehouse/reachability-metadata.json`
     (5,384 bytes).
   - Writer: `//warehouse:update_reachability_metadata` (testonly, `WH:398-404`). Diff test:
     `//warehouse:update_reachability_metadata_test`.
   - Suites: `//:generated` (`ROOT:95`) and `//:update_generated` (`ROOT:123`).
8. **Who consumes it.**
   - **Product at build time:** native-image reads the committed file from `server_lib`'s jar when it builds
     `//warehouse:server_native` (`WH:71-75,192-213`).
   - The JVM server does not read it at run time, although it ships inside `server_lib`'s jar, so `//:java` carries it.
   - **Tests:** only its diff test.
   - **Humans:** `WH:384-387,401`; `docs/WAREHOUSE_W1_DESIGN_2026_09_26.md:346`; `Duck.java:85-87`;
     `AuthenticatedUser.java:57`.
9. **Determinism.** Yes. Downcalls in a `TreeSet`, upcalls sorted (`:85-94,98-99`), declared lists in fixed order, and
   no host data. The notation is platform-independent by construction (`:21-23`). One copy passes on 4 platforms.
10. **Cost.** One small JVM. No DuckDB loaded and no server.
11. **What reruns it today.**
    - Any edit in `warehouse/src/test/java/**` (except `launcher/` and `sqlapi/`), `warehouse/src/main/java/.../server/**`,
      `sqlapi`, `client`, `//testing`, `//base`, `//json`, the warehouse resources (including its own committed output),
      the JUnit pins, and the DuckDB jar pin.
12. **What SHOULD rerun it.** `Duck.java` (`DOWNCALLS`, `RELEASE`), `AuthenticatedUser.java` (`UPCALLS`), and the
    `SERVICES`/`RESOURCES` tables in `ReachabilityMetadata.java`.
13. **Who runs it today.**
    - `bazel build //...` (testonly targets are built); `//gates:local` (`//:generated →
      update_reachability_metadata_test`).
    - The `checks` lane; the bump (`//:update_generated`), with no upstream input.
    - By hand: `bazel run //warehouse:update_reachability_metadata`.
14. **Recommendation: COMMITTED-SOURCE.** It is product build input for the native image, derived from named files of
    ours.
    - Manual: yes.
    - Writer: `//:update_generated`. Diff test: the everyday gate.
    - **Narrowing** (as design E says): its own testonly library, `srcs = [ReachabilityMetadata.java]`,
      `deps = [":server_lib"]`. The runtime closure is then `server_lib`, `sqlapi`, `base`, `json` and the 2 runfiles
      jars (the runfiles pair leaves with D9).
    - Break the self-loop by moving the native-image JSON out of `server_lib`'s resources into a resource-only library
      that only `server_native` (and, if wanted, `server`) depends on.
15. **Open questions.** None.

## 15. //wasm:jvm_answers

1. **Identity.**
   - A `java_run` at `W:96-111`: srcs `corpus/model.pure`, `corpus/queries.tsv`; args `… trades::RT {OUT}` (Pure-text
     mode).
   - Program: `planner.JvmMain` (`JvmMain.java:32-55`), deps `:jvm_main` (`W:76-83`), which depends on `:boundary`
     (`W:33-49`), which depends on `//core:plan_side` and `//json`.
2. **What it computes.** For each of the 69 lines of `wasm/corpus/queries.tsv`, `Wasm.planOrError(model, query,
   "trades::RT")` (`Wasm.java:122-129`, through `planTyped`, `:105-111`): `OK\n{sql,type}` or `ERR\n<class>\n<message>`.
3. **Why it exists.**
   - `b02032e69` (2026-09-23); made a target-configuration `java_run` in `e1c92a30e`.
   - It is the JVM half of the planner differential: the same `Wasm` class is compiled by TeaVM and called by JvmMain,
     "so the differential compares two builds of one source" (`W:31-32,93-95`; `wasm/differential.mjs:5`).
4. **Inputs, declared** (aq, 148 inputs): 28 jars (24 plan-side core libraries, base, json, boundary, jvm_main), the 2
   corpus files, and 118 JDK files.
5. **Inputs, actually read.**
   - `Compiler` (planner), `QueryPlan` and `UpstreamRelationType` (plan), and `Json`. Planner closure: 24.
   - It reads the 2 argv files and writes the third.
   - **Over-declared:** `diagnostics` and `execution_plan` (from `plan_side`).
6. **Outputs.** `wasm/jvm_answers.txt`.
7. **Committed?** No. Read by `//wasm:differential_test` (`W:113-126`) from beside its script
   (`wasm/differential.mjs:41`, `here('./jvm_answers.txt')`).
8. **Who consumes it.** `differential_test` only (rdeps1). **Humans:** `wasm/README.md:20-21`.
9. **Determinism.** Yes. TSV order, and answers are text. The plan's records do not note a failure.
10. **Cost.** One JVM, 69 plans. No database (the plan side has no driver).
11. **What reruns it today.**
    - Every plan-side core jar, `Wasm.java`, `JvmMain.java`, the corpus files, and the JDK.
    - `q 'somepath(//wasm:jvm_answers, //core:src/main/java/com/legend/Compiler.java)'` gives
      `jvm_answers → jvm_main → boundary → //core:plan_side → //core:planner → Compiler.java`. That is expected.
12. **What SHOULD rerun it.** Engine behaviour (the planner) and `wasm/corpus/*`. It is a test oracle.
13. **Who runs it today.**
    - `bazel build //...`; `//gates:local` (`//wasm:differential_test`).
    - The CI `app` lane (`//wasm:all`); the bump's `test //...`.
14. **Recommendation: BUILD-OUTPUT, testonly** (design group E).
    - Manual: yes.
    - No writer. Its test stays where it is (everyday gate, `app` lane).
    - No narrowing worth the cost (2 jars).
15. **Open questions.** None.

## 16. //wasm:zone_jvm

1. **Identity.**
   - A `java_run` at `W:130-136`, args `{OUT}`.
   - Program: `planner.ZoneMain` (`wasm/src/main/java/planner/ZoneMain.java:29-37`), deps `:zone_main` (`W:85-91`), which
     depends on `:boundary` and so on `//core:plan_side`.
2. **What it computes.** For 8 fixed (UTC instant, zone) cases (`ZoneMain.java:17-26`), it records
   `Wasm.zoneProbe(iso, zone)` (`Wasm.java:405-411`), which is `com.legend.lowering.LiteralSpelling.inZone`, the
   planner's only `ZoneId.of`. One `iso\tzone\tanswer` line each.
3. **Why it exists.** `b02032e69` (2026-09-23). A tz database is a resource, not code, so only the built WASM module can
   say whether it survived TeaVM. ZoneMain gives the JVM's answers to compare (`ZoneMain.java:3-11`, `W:128-129`).
4. **Inputs, declared** (aq, 146 inputs): 28 jars (24 plan-side core, base, json, boundary, zone_main) and 118 JDK files.
   The JDK files include its tz database.
5. **Inputs, actually read.**
   - `LiteralSpelling` (`//core:lowering`, closure 15 libraries) and the exec JDK's tz rules (through `ZoneId.of`).
   - It loads the `Wasm` class to call `zoneProbe`; whether verifying `Wasm` needs the planner classes is open question
     1.
   - It writes `args[0]`.
   - **Over-declared** (if called directly): 11 plan-side jars beyond lowering's closure: cache, database, diagnostics,
     execution_plan, lineage, normalizer, plan, planner, resolver, sql_dialect, validation.
   - **Under-declared:** none. The JDK is declared.
6. **Outputs.** `wasm/zone_jvm.txt`.
7. **Committed?** No. Read by `//wasm:zone_test` (`W:138-147`, `wasm/zoneprobe.mjs:19`).
8. **Who consumes it.** `zone_test` only (rdeps1). **Humans:** `wasm/README.md:22`.
9. **Determinism.** Yes for a given JDK. The answers depend on the exec JDK's tzdata (the pinned `remotejdk25`), so a
   JDK bump with new zone rules can move them.
10. **Cost.** A tiny JVM, 8 calls. No database.
11. **What reruns it today.**
    - Every plan-side core jar, `Wasm.java`, `ZoneMain.java`, and the JDK.
    - `q 'somepath(//wasm:zone_jvm, //core:src/main/java/com/legend/Compiler.java)'` gives
      `zone_jvm → zone_main → boundary → //core:plan_side → //core:planner → Compiler.java`, a class it never calls.
12. **What SHOULD rerun it.** `LiteralSpelling` (and its 15-library closure), the zone cases, and the JDK's tz rules.
13. **Who runs it today.**
    - `bazel build //...`; `//gates:local` (`//wasm:zone_test`).
    - The CI `app` lane (`//wasm:all`); the bump's `test //...`.
14. **Recommendation: BUILD-OUTPUT, testonly.**
    - Manual: yes.
    - No writer.
    - Narrowing (design: "narrowed to LiteralSpelling"): ZoneMain calls `LiteralSpelling.inZone` directly, with deps
      `//core:lowering` (closure 15). The cost is that the JVM half no longer goes through `Wasm.zoneProbe`'s try/catch,
      which is two lines to mirror.
15. **Open questions.**
    1. With only `//core:lowering` on the class path, does loading `Wasm` (to keep the shared entry) fail verification?
       Settle by running `ZoneMain` with that class path once, or by moving `zoneProbe` into its own small boundary
       class that both targets use.

---

## Cross-cutting findings

1. **Every JVM generator here depends on "all of core" or more, except the two pins.**
   - `catalog_*`, `offer_facts` and `lite_facts` take all 31 core jars, and `reachability_metadata` takes the warehouse's
     whole test library.
   - The true sets are 3 (catalog rules), 12 (catalog corpus), 24 (offer facts) and 13 (lite facts) libraries, and
     `server_lib` for reachability.
   - Only `catalog_corpus` starts DuckDB. No generator in G5 starts a server or Postgres.
2. **The JS generators declare all of `:src` plus 122 npm files** but import 33 to 48 files and no npm package. A
   generated "tool imports" list, as `TEST_IMPORTS` is, or the Java port of design §4.6, fixes that.
3. **Two generators read a committed generated file.**
   - `link_dictionary_next` truly depends on `offer-facts.ts`.
   - `reachability_metadata` reads its own committed output through `server_lib`'s resources (an idempotent self-loop).
4. **`//datacube:dist` is broken as a deployable on this branch.** It lacks fonts and is made of absolute symlinks. The
   completeness fix is `dab833263` on `bazel/exec`. D10 retires it.
5. **Stale references to fix.**
   - `MODULE.bazel:320` (`//query:icons_gen`);
   - `docs/GATES.md:46-49` (`//query:update_generated_test`, "Query's icons.ts", "DataCube's lite-facts.ts");
   - `TypeFacts.java:26-27` (`//datacube:type_facts`).
6. **The bump regenerates files with no upstream input.** `test_imports`, `icons_gen` and `reachability_metadata` are
   rerun by `Bump.java` through `//:update_generated` today. They belong to the everyday update, not a bump-only one.

## Summary table

| label | recommendation | manual? | update group | true trigger |
|---|---|---|---|---|
| //datacube:catalog_rules | COMMITTED-SOURCE | yes | //:update_generated (C/D) | `C/sql/dialect/{CatalogRules,CatalogType,DuckDb,Postgres}.java` (`//core:sql_dialect`), CatalogFacts.java |
| //datacube:catalog_corpus | TEST-IN-DISGUISE | yes | none (testonly output read by catalog_model_test; or an explicit re-pin) | engine behaviour: CatalogModel, Databases, parser and protocol emitter (12 libraries), and DuckDB 1.5.5.1's catalog (jar pin) |
| //datacube:offer_queries | BUILD-OUTPUT (testonly-able) | yes | none | emit.ts and its 33-file closure (DataCube query builder, snapshot, calc list, pure-protocol, engine-client types) |
| //datacube:offer_facts | COMMITTED-SOURCE | yes | //:update_generated (C/D) | our compiler (`//core:planner` closure, 24 libraries) plus offer_queries' bytes plus OfferFacts.java |
| //datacube:test_imports | COMMITTED-SOURCE | yes | //:update_generated (C/D) | import lines of `datacube/src/**/*.ts` and `datacube/test/**/*.ts` |
| //datacube:link_dictionary_next | DRAFT-MANUAL | yes | none (`cut_link_dictionary` by hand, once per version) | a human cutting the next share-link version |
| //datacube:cube_queries | BUILD-OUTPUT, testonly | yes | none | cases.ts, emit.ts and their 34-file product closure |
| //datacube:cube_jvm_answers | BUILD-OUTPUT, testonly | yes | none | engine behaviour (planner closure) plus the cube cases |
| //datacube:dist | BUILD-OUTPUT (packaging), replaced by D10's shared `:site` rule and `//site:dist` | no (packaging tier `//:sites`) | none | the shipped bundles, vendor runtimes, page html/css |
| //engine-client:lite_facts | COMMITTED-SOURCE | yes | //:update_generated (C/D) | `C/compiler/element/type/*` (`//core:compiler_element_type`, 13 libraries, includes generated Pure.java), TypeFacts.java |
| //legend-art:icons_gen | COMMITTED-SOURCE | yes | //:update_generated (C/D) | the `ICONS` table in `legend-art/tools/icons.mjs` and `_ICON_SETS`; rarely the `@react_icons` pin (not in tools/bump) |
| //warehouse:duckdb_library | BUILD-OUTPUT (product data) | no | none | `duckdb_jdbc_warehouse` in tools/deps/jars.bzl |
| //warehouse:duckdb_extensions | BUILD-OUTPUT (product data) | no | none | `duckdb_postgres_extension_*` pins (MODULE.bazel:358-372), Gunzip.java |
| //warehouse:reachability_metadata | COMMITTED-SOURCE | yes | //:update_generated (C/D) | Duck.java (DOWNCALLS, RELEASE), AuthenticatedUser.java (UPCALLS), the SERVICES/RESOURCES tables |
| //wasm:jvm_answers | BUILD-OUTPUT, testonly | yes | none | engine behaviour (planner closure) plus `wasm/corpus/*` |
| //wasm:zone_jvm | BUILD-OUTPUT, testonly | yes | none | `LiteralSpelling` (`//core:lowering`), the zone cases, the JDK's tz rules |
