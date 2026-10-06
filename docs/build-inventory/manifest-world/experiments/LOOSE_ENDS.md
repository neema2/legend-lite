# Loose ends: toString, the 13 untraced prelude names, the 19 test-only prelude names

Read-only research, 2026-10-06, branch `build/rebuild` at `1689703f2`. Pins: legend-engine 4.145.0 /
legend-pure 5.99.0 (`release.MODULE.bazel:24-25`). Terminology follows `docs/UPSTREAM_BOUNDARY_PROGRAM.md` §0
(lines 20-51): "upstream native" = upstream's `native function` keyword (a Java body); "platform-lowered" = declared
in our `Pure.java`.

Abbreviations used below:

- `PURE/` = the pinned legend-pure tree named in `runs/homework/world/pt`
- `ENGINE/` = the pinned legend-engine tree named in `runs/homework/world/et`
- `PG` = `spec/src/gen/java/com/legend/generators/PreludeGenerator.java`
- `UF` = `spec/src/gen/java/com/legend/generators/UpstreamFiles.java`
- `prelude` = `core/src/main/resources/com/legend/builtin/prelude.pure`
- `B/` = `core/src/main/resources/com/legend/builtin/`

Repo paths are relative to `<repo>`. Negative findings ("no use") are
from `git grep` over the whole repo excluding `prelude.pure` and `runs/`, plus `grep -r` over `core/src`,
`spec/src`, `pct/src` and `parser-equivalence/src`. The commands are given where it matters.

---

## 1. `meta::pure::functions::string::toString`

**Answer.** It is an upstream function. It is an **upstream native** declared in legend-pure (not in m3.pure), and
its signature id is **`meta::pure::functions::string::toString_Any_1__String_1_`**. In §0's table it sits in the
"upstream native + we lower it" cell (`docs/UPSTREAM_BOUNDARY_PROGRAM.md:37-39`). Nothing here is OPEN.

**Our declaration.** `core/src/main/java/com/legend/builtin/Pure.java:2200`:
`TO_STRING__ANY_1 = signature("native function meta::pure::functions::string::toString(any:meta::pure::metamodel::type::Any[1]):meta::pure::metamodel::type::String[1];")`.

**Upstream declaration.** It has exactly one declaration across both pinned trees:
`PURE/legend-pure-core/legend-pure-m3-core/src/main/resources/platform/pure/essential/string/toString/toString.pure:47-49`.

```
native function
    <<PCT.function>>
    meta::pure::functions::string::toString(any:Any[1]):String[1];
```

- A documentation block comes first (`toString.pure:19-46`). The file's `<<PCT.test>>` functions follow
  (`toString.pure:51-182`).
- The earlier searches missed it for two reasons. First, `m3.pure` really has no "toString" text (`grep -c` gives 0
  on `PURE/legend-pure-core/legend-pure-m3-core/src/main/resources/platform/pure/grammar/m3.pure`). Second, the
  keyword, the stereotype and the FQN sit on three separate lines, so a single-line grep for `function ...::toString(`
  cannot match.
- Our own generator does not have this problem. `NativesGenerator.FUNC_HEADER`
  (`spec/src/gen/java/com/legend/generators/NativesGenerator.java:269-271`) lets `\s` span newlines. Running that
  same regex over `toString.pure` matches `meta::pure::functions::string::toString` at line 47.
- legend-engine declares no `meta::pure::functions::string::toString`. A multi-line-aware search of `ENGINE/` finds
  only `toString` functions in other packages. One example is `meta::pure::functions::relation::toString` at
  `ENGINE/legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-functions-relation/legend-engine-pure-functions-relation-pure/src/main/resources/core_functions_relation/relation/functions/toString.pure:19,24`,
  which `Pure.java:1634-1635` declares as separate entries.

**The Java side upstream.** The Java code implements the Pure-declared upstream native; it does not declare it.

- Compiled runtime: `PURE/legend-pure-runtime/legend-pure-runtime-java-engine-compiled/src/main/java/org/finos/legend/pure/runtime/java/compiled/generation/processors/natives/essentials/string/toString/ToString.java:21-27`
  (key `"toString_Any_1__String_1_"`). It is registered at `.../compiled/generation/processors/NativeFunctionProcessor.java:544`.
- Interpreted runtime: `PURE/legend-pure-runtime/legend-pure-runtime-java-engine-interpreted/src/main/java/org/finos/legend/pure/runtime/java/interpreted/FunctionExecutionInterpreted.java:578`
  registers `.../interpreted/natives/essentials/string/toString/ToString.java:38`.
- Engine compile-time handler:
  `ENGINE/legend-engine-core/legend-engine-core-base/legend-engine-core-language-pure/legend-engine-language-pure-compiler/src/main/java/org/finos/legend/engine/language/pure/compiler/toPureGraph/handlers/Handlers.java:2659`
  has `register("meta::pure::functions::string::toString_Any_1__String_1_", "toString", true, ps -> res("String", "one"))`.
- Id and signature agree: `PURE/legend-pure-core/legend-pure-m3-core/src/test/java/org/finos/legend/pure/m3/tests/function/TestFunctionDescriptor.java:117`
  maps `toString(Any[1]):String[1]` to `meta::pure::functions::string::toString_Any_1__String_1_`.

**Our rows.**

| file | line | content |
|---|---|---|
| `B/native-membership.tsv` | 704 | `TO_STRING__ANY_1  meta::pure::functions::string::toString  ...toString(meta::pure::metamodel::type::Any[1])` |
| `B/native-claims.tsv` | 745 | kinds `SCALAR_RULE`, owners `Scalars.RULES`, also `DynaFn;ExecuteChainAssembly;OrderView;PlatformTypes;Scalars;StaticFold` |
| `B/engine-handlers.tsv` | 789 | `toString  meta::pure::functions::string::toString_Any_1__String_1_  meta::pure::functions::string::toString  engine` |
| `prelude` | 6829 | listed under PLATFORM-OWNED NAMES (header `prelude:6600-6601`), so it is **not carried** |

Our PCT lane exercises it. For example, `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_EssentialFunctions_PCT.java:76`
pins `testComplexClassToString` as an expected failure.

---

## 2. The 13 names nobody could trace

**Answer.** All 13 come from one rule, decided by the user at batch 3: *"§6.2 — DECIDED: the prelude carries
**all** upstream natives in the read roots"* (`docs/CLAIM_REGISTRY_DESIGN_2026_09_10.md:227-228`). The doc's status
line records the approval (`:3-5`). The rationale was *"'unknown function' is never the answer for a function upstream
declares"* (`:216-217`); see also `docs/UPSTREAM_BOUNDARY_PROGRAM.md:571, 703-706`.

- **4** of the names are engine `native function`s, carried respelled.
- **7** are classes that those upstream natives' signatures name.
- **2** come in by closure from those classes.

None of the 13 enters through Java demand or corpus demand, and nothing in our code, tests or corpus rosters names any
of them.

### Why the generator includes them

1. **Roots.** `ENGINE_SPEC_ROOTS` (`PG:139-145`) covers all of `legend-engine-xts-relationalStore`, all of
   `legend-engine-core/legend-engine-core-pure`, and `core_service`. All four declaring files sit under these roots.
   None of them is under `PLATFORM_ROOTS` (`UF:151-160`) or `STDLIB_ENGINE_ROOTS` (`UF:142-147`).
   - `STDLIB_ENGINE_ROOTS` is documented as upstream's own "core" (`UF:136-141`). That matches
     `PURE/legend-pure-core/legend-pure-m3-core/src/main/java/org/finos/legend/pure/m3/serialization/runtime/PureRuntime.java:248`,
     which treats repositories named `platform*` or `core_functions*` as core.
   - The four declaring modules are `core_external_language_java_compiler`, `pure_ide_debug`,
     `core_external_store_relational_postgres_sql_parser` and `core_external_store_relational_sdt`. None of them is
     core by upstream's own rule.
2. **Engine-declared upstream natives.** `engineNatives` (`PG:1380-1412`) parses every `.pure` file under those roots that matches
   `native function` (`PG:1388`), and keeps only the upstream natives.
3. **Ownership.** An upstream native is carried respelled unless `Pure.java` declares its FQN, its bare name is claimed, or it
   is a CoreFn form (`PG:573-592`). None of the 13 is owned: `grep -c` gives 0 for each in `B/native-membership.tsv`,
   `B/native-claims.tsv` and `B/engine-handlers.tsv`. The prelude header describes the result: "carried so they
   resolve and type-check; a call fails at lowering as 'not implemented'" (`prelude:6588-6589`).
4. **Signature seeding.** `PG:399-405` and `PG:456-501` handle this, using `referencedTypeNames` (`PG:1414-1429`).
   - `compileJava` (`prelude:5289`) names `JavaSource`, `CompilationConfiguration` and `CompilationResult`.
   - `compileAndExecuteJava` (`prelude:5291`) also names `ExecutionConfiguration` and `CompileAndExecuteResult`.
   - Each is resolved in the declaring package (tier 0, `PG:469-483`) and seeded before the closure (`PG:497`).
5. **Closure.** `Spec.close` (`PG:909-930`) walks property types:
   - `CompileAndExecuteResult.executionResult : ExecutionResult[0..1]` (`prelude:5279`) brings in **ExecutionResult**.
   - `ExecutionResult.returnValue : JavaValue[0..1]` (`prelude:5269`) brings in **JavaValue**.
   - The closure follows referenced types, not subtypes. So upstream's `JavaNull` … `JavaObject` (upstream
     `compiler.pure:47-106`) are not carried; the section `prelude:5243-5291` ends at the two upstream natives.
6. **Java demand has no role.** The Java scan (`PG:331-357`) finds none of the 13. A grep of
   `core/src/main/java` for `java::compiler`, `postgresSql::parser`, `sdt::framework` and `pure::ide` returns
   nothing. The other upstream natives' signatures seed nothing new:
   - `parsePostgresDate` takes and returns primitives (String to Date).
   - `parseSqlStatementToJson` is String to String.
   - `ide::debug` returns `Nil[0]`.
   - `runSqlDialectTestQuery` returns `meta::relational::metamodel::execute::ResultSet`, which is already Java
     vocabulary: `Pure.java` names it, and it sits at `prelude:3042`.

### Per name

Upstream paths:

- `JC` = `ENGINE/legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-functions-javaCompiler/legend-engine-pure-functions-javaCompiler-pure/src/main/resources/core_external_language_java_compiler/compiler.pure`
- `PSP` = `ENGINE/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-postgresSql/legend-engine-xt-relationalStore-postgresSqlParser-pure/src/main/resources/core_external_store_relational_postgres_sql_parser/`
- `SDT` = `ENGINE/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-SDT-pure/src/main/resources/core_external_store_relational_sdt/sdtFramework.pure`
- `DBG` = `ENGINE/legend-engine-core/legend-engine-core-pure/legend-engine-pure-ide/legend-engine-pure-ide-light-pure-debug/src/main/resources/pure_ide_debug/debug.pure`

| name | prelude | upstream | route | added to prelude |
|---|---|---|---|---|
| `java::compiler::compileJava` | 5289 | `JC:139` | upstream native (engine), respelled | `aca92fb2b` |
| `java::compiler::compileAndExecuteJava` | 5291 | `JC:141` | upstream native (engine), respelled | `aca92fb2b` |
| `java::compiler::JavaSource` | 5282 | `JC:122` | signature seed (both upstream natives) | `aca92fb2b` |
| `java::compiler::CompilationConfiguration` | 5248 | `JC:19` | signature seed (both upstream natives) | `aca92fb2b` |
| `java::compiler::CompilationResult` | 5253 | `JC:24` | signature seed (compileJava's return) | `aca92fb2b` |
| `java::compiler::ExecutionConfiguration` | 5259 | `JC:30` | signature seed (compileAndExecuteJava) | `aca92fb2b` |
| `java::compiler::CompileAndExecuteResult` | 5276 | `JC:116` | signature seed (compileAndExecuteJava's return) | `aca92fb2b` |
| `java::compiler::ExecutionResult` | 5265 | `JC:36` | closure via CompileAndExecuteResult | `aca92fb2b` |
| `java::compiler::JavaValue` | 5272 | `JC:43` | closure via ExecutionResult | `aca92fb2b` |
| `postgresSql::parser::parsePostgresDate` | 5632 | `PSP/parsePostgresDate.pure:17` | upstream native (engine), respelled | `0a4a928c6` |
| `postgresSql::parser::parseSqlStatementToJson` | 5641 | `PSP/postgresSqlParser.pure:20` (`<<access.private>>`) | upstream native (engine), respelled | `aca92fb2b` |
| `sdt::framework::runSqlDialectTestQuery` | 5658 | `SDT:30` | upstream native (engine), respelled | `aca92fb2b` |
| `meta::pure::ide::debug` | 5626 | `DBG:15` | upstream native (engine), respelled | `aca92fb2b` |

The "added" column comes from `git log -S'<declaration text>' -- prelude`.

### Commits

- **`aca92fb2b`** (2026-09-11), "Batch 4 §6.2 completed: the engine's natives enter the prelude respelled".
  - It added `engineNatives`, `referencedTypeNames` and the signature seeding. At the time these lived in
    `core/src/test/java/com/legend/tools/PreludeGeneratorTest.java`. They moved with `998f1a41c` and reached
    `spec/src/gen` at `fb43cd0a8`.
  - Its message uses `compileJava(classes:JavaSource[*], config:CompilationConfiguration[0..1])` as the worked
    example and reports respelled upstream natives going from 37 to 67.
  - `docs/GATES.md:392` lists `compileJava`/`compileAndExecuteJava`, `debug`, `parseSqlStatementToJson` and
    `runSqlDialectTestQuery` among the +30.
- **`0a4a928c6`** (2026-09-11), "Batch 8: the bump to legend-engine 4.145.0 / legend-pure 5.99.0". It is the only
  commit in the repo that mentions `parsePostgresDate`, and the bump came from 4.138.2 / 5.92.0 (the oracle-pins
  diff in that commit).
  - Evidence that the file is new upstream: the gate-8 corpus manifest before the bump (`0a4a928c6^`) lists the
    sibling `postgresSqlParser.pure` (line 2031) but not `parsePostgresDate.pure`. After the bump it lists both
    (`parser-equivalence/src/test/resources/corpus-manifest.tsv:2096-2097`).
  - OPEN: whether the file was added upstream between 4.138.2 and 4.145.0. The pinned tree is an http_archive with
    no git. To settle it, run `git log --diff-filter=A` on that path in a legend-engine clone.

### Uses in our repo: none

- **Repo-wide git grep.** The only hits are:
  - the batch log at `docs/GATES.md:392`;
  - a comment at `PG:400`;
  - gate-8 manifest rows that list the declaring *files* as parser input
    (`parser-equivalence/src/test/resources/corpus-manifest.tsv:663, 872, 2096-2098`);
  - a false positive on "JavaSourceJar" in `tools/guards/CompileOnlyTest.java:35,44`.
- **Corpus.** Nothing under the relational corpus root (`PG:149-152`) calls any of them. The four core-pure
  `LIBRARY_FILES` (`UF:53-56`) do not either. In `SHAPE_FILES`, the only `debug()` calls
  (`executionPlan_generation.pure:52, 223`) are `meta::pure::tools::debug`, which our core imports reach
  (`core/src/main/java/com/legend/compiler/NameResolver.java:243`). They are not `meta::pure::ide::debug`.
- **Rosters and pins.** No fail roster, unordered register or PCT expected-failure list names any of the 13.
- **Upstream's own callers are engine-only.**
  - `compileJava` is called by `JC:129-137` (the bodied overloads, which are not carried) and by
    `ENGINE/legend-engine-xts-changetoken/.../cast_generation_test.pure`.
  - `ide::debug` has no callers.
  - `runSqlDialectTestQuery` is called by `SDT:37`.
  - `parsePostgresDate` is called by the 27 `<<test.Test>>` functions in its own file and by
    `ENGINE/legend-engine-xts-sql/.../core_external_query_sql/binding/fromPure/fromPure.pure`.
  - `parseSqlStatementToJson` is called by `PSP/postgresSqlParser.pure:25`.

**One indirect consumer: the reference lane**, a manual heavy lane.

- `OurResolutions` loads `core_relational`'s manifest closure. It drops every parsed `native function`
  (`spec/src/test/java/com/legend/generators/OurResolutions.java:69-70`) and types every body (`:99-123`).
- The model always includes the boot layer, that is the system metamodel plus the prelude
  (`core/src/main/java/com/legend/Compiler.java:218-225, 268`).
- The postgres-parser module is in that closure: its `parseSqlStatement` appears in the golden's failed bodies
  (`spec/src/test/resources/reference-lane/core_relational.txt:148`).
- So the 27 test bodies in `parsePostgresDate.pure`, which are not in the failed list, can resolve
  `parsePostgresDate` only through the prelude's respelled copy.
- `parseSqlStatement` already fails, because `fromJsonNative` is not carried: it is listed under ENGINE NATIVES NOT
  CARRIED (header `prelude:6590-6591`, entry `prelude:6593`).
- OPEN: the exact golden delta if either upstream native were dropped. To settle it, run `//spec:reference_lane_report` on a
  branch without them.

### Could they be dropped?

As far as use goes, yes: nothing in our product, tests, corpus or rosters names them, and the 9 `java::compiler`
names exist only to close the two upstream natives' signatures.

They are there by a user-approved rule (§6.2), so dropping them is a decision, not a cleanup. Dropping means
narrowing which roots feed respelled upstream natives, for example to `PLATFORM_ROOTS` plus `STDLIB_ENGINE_ROOTS`
(upstream's own core). That is a generator change: `prelude.pure` is generated and checked for currency
(`PG:62-67`), so it cannot be hand-edited. Known costs:

- A call to one of them would report "unknown function" instead of "not implemented", which is exactly what §6.2
  rejected.
- The reference-lane golden would move for the `parsePostgresDate` tests (OPEN, above).

---

## 3. The 19 test-only names

**Answer.**

- Nothing in our repo uses **18** of them: the PCT manifest helpers and the surveyor.
- They sit in every program's default world (`Compiler.java:218-225, 268`). Two quirks of how the generator draws
  its "test" line put them there:
  - a class exemption whose stated reason disappeared at `3dd586e60`;
  - a filter for functions and upstream natives that only recognizes the plural `::tests::`.
- They belong at most in the world of a program that runs Pure tests through the surveyor, and no such program
  exists here. Our PCT program does not need them from the prelude.
- **`TestedByResult` is different.** It is engine main code, named by the verbatim shape of
  `meta::pure::extension::Extension`, which our Java names. It stays in the default world as long as `Extension`
  does.

### Who uses them in our repo

- **Core Java: none of the 19.** The only `meta::pure::test::` mention is
  `core/src/main/java/com/legend/compiler/element/type/PlatformTypes.java:433`
  (`PCT_PROFILE = "meta::pure::test::pct::PCT"`).
  - That is the Profile, declared at `PURE/.../essential/tests/pct_core.pure:17`. It is not one of the 19.
  - The generator does not index it either: `DECL_HEADER` matches only `Class|Enum` (`PG:1636-1637`).
- **The `pct/` module: none of the 19.**
  - **Channel A** builds its suite with upstream's Java discovery:
    `Test_LegendLite_EssentialFunctions_PCT.java:14,160` calls `PureTestBuilderInterpreted.buildPCTTestSuite`. That
    uses `TestCollection` plus a Java executor
    (`PURE/legend-pure-runtime/legend-pure-runtime-java-engine-interpreted/src/main/java/org/finos/legend/pure/runtime/java/interpreted/testHelper/PureTestBuilderInterpreted.java:80-125`),
    which runs inside upstream's own runtime and platform. Our side receives only grammar text through
    `executeLegendLiteQuery` (`pct/src/main/resources/core_legend_lite_pct/pct_native.pure:13`). The packer injects
    only test-model classes and `::tests::` support functions
    (`pct/src/test/java/org/finos/legend/lite/pct/extension/ModelPacker.java:199-226`).
  - **Channel B** uses the identity adapter (`pct/src/test/java/org/finos/legend/lite/pct/channelb/ChannelB.java:24-31`).
    Its model is the legend-pure platform tree loaded as program files (`:108`, `ChannelBEssentialTest.java:32-35`),
    with every parsed `native function` dropped (`ChannelB.java:136-142`). It runs only `<<PCT.test>>` functions (`:195`, `:212-217`).
  - No PCT test calls any of the 18. Upstream, only `pct_core.pure`, `surveyor.pure` and a comment
    (`PURE/legend-pure-store/.../platform_store_relational/tests/h2_round_trip.pure:24`) mention them.
- **Corpus runner: none of the 18.** `TestedByResult` is named by corpus code:
  `ENGINE/legend-engine-xts-relationalStore/.../core_relational/relational/extensions/extension.pure:234` (the
  relational `Extension`'s `testExtension_testedBy` lambda).
- **Other mentions** are evidence of the names, not uses:
  - The reference-lane golden has rows for calls *inside* the surveyor's own bodies
    (`spec/src/test/resources/reference-lane/core_relational.txt:1647-1648, 1725, 1758-1760, 3389-3398`). The lane
    types every body (`OurResolutions.java:99-123`).
  - The 2026-09-08 census marks `PCTManifest` and `TestResult` as demand "java" (`docs/PRELUDE_MODULE_CENSUS_2026_09_08.tsv:306,309`).
    That was true when `Pure.java` declared the three harness functions as platform-lowered (added `1555b13ce`, removed `3dd586e60`).
- **Upstream's own surveyor user** is a different entry point, `PureTestBuilderInterpreted.executeSurveyorTests`
  (`PureTestBuilderInterpreted.java:202-226`, which calls `runTestsFromPath`). Its three upstream natives have Java bodies in the
  runtime (`.../natives/essentials/tests/{ExecuteTest,ExecutePCTTest,LoadPCTManifest}.java` in both the
  compiled and the interpreted runtime).

### Why the generator includes them

`T` = `PURE/legend-pure-core/legend-pure-m3-core/src/main/resources/platform/pure/essential/tests/`. This directory
is under `PLATFORM_ROOTS[0]` (`UF:152`).

| names | prelude | upstream | route | in prelude since |
|---|---|---|---|---|
| `pct::PCTManifest`; `surveyor::TestResult`, `TestStatus` | 1411; 1424, 1432 | `T/pct_core.pure:46`; `T/surveyor.pure:19, 27` | T1 seed: every class/enum under the platform roots (`PG:389-398`), let through by the `meta::pure::test::` exemption in `excluded()` (`PG:1628-1631`) | `4cfc206ee` (batch 151, the file's birth) |
| `surveyor::TestReport`, `TestGroup` | 1440, 1580 | `T/surveyor.pure:35, 203` | same | `a1dd039d0` (batch 154, platform packages whole) |
| `pct::testAdapterForInMemoryExecution`; `surveyor::runTests`, `runTestsFromPath`, `runPCTTests`, `runPCTTestsFromPath`, `getTestFunctions`, `getPCTTestFunctions`, `getUpstreamPackages`, `buildTestGroup`, `flattenTestGroup` | 1401-1409; 1454, 1501, 1507, 1548, 1554, 1567, 1589, 1594, 1610 | `T/pct_core.pure:29-37`; `T/surveyor.pure:66, 113, 124, 167, 176, 190, 212, 217, 233` | platform library functions, bodied and non-test (`PG:508-572`). The test filter (`PG:1532-1542`) drops only test stereotypes and FQNs containing `::tests::`; these live in `meta::pure::test::` (singular) and have no test stereotype (the adapter's is `<<PCT.adapter>>`) | `d961be36e` (batch 169) |
| `pct::loadPCTManifest`; `surveyor::executeTest`, `executePCTTest` | 1417; 1450, 1452 | `T/pct_core.pure:55`; `T/surveyor.pure:51, 60` (upstream natives) | respelled upstream natives (`PG:549-564`). The slice for `native function` declarations skips only `::tests::` (`PG:1506`) | `3dd586e60` (batch 4a, when they left `Pure.java`) |
| `functions::test::TestedByResult` | 4042 | `ENGINE/legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-compiled-core/src/main/resources/core/pure/corefunctions/testExtension.pure:114` | closure (`PG:909-930`, which honours only `excludedByDecision`) from `meta::pure::extension::Extension` (`prelude:4332`). Its property `testExtension_testedBy` (`prelude:4382`) types over `TestedByResult`. `Extension` is Java vocabulary (`Pure.java:1329, 1404-1405`; `SystemMetamodel.java:1128, 1136`). It is also a T4 receipt (`prelude:6938`) because `SHAPE_FILES` admits `testExtension.pure` (`UF:81`) | `4cfc206ee` |

**The exemption's reason is stale.** The comment at `PG:1629-1630` says the exemption exists because "the natives
executeTest/executePCTTest/loadPCTManifest name its shapes (batch 150)".

- That was true from `1555b13ce` (2026-09-08). That commit put `PCT_EXECUTE_TEST__1`, `PCT_EXECUTE_PCT_TEST__3` and
  `PCT_LOAD_MANIFEST__1` in `Pure.java` ("typed, never run here") and added the exemption.
- It stopped being true at `3dd586e60` (2026-09-10), which removed those three from `Pure.java`.
- The parallel exemption in the Java-demand scan (`PG:348-353`) admits nothing today. Core's Java names no
  `meta::pure::test::` class or enum.

**What the generator says it intends.**

- Test support is "the PCT lane's world, not the library's" (`PG:1537-1539`; also `PG:1360-1363`).
- `d961be36e`'s message: "Three receipt lists keep the rest out: tests packages (test support), …".
- The surveyor is test support by that principle. It gets through only because the package is spelled `test`, not
  `tests`.

### Do they belong in a default world?

**The 18: no.** This verdict rests on the generator's stated principle above and on what the code does.

- **They are inert in a user program.** The three upstream natives have no implementation on our side: they are respelled
  (`prelude:6588-6589`) and have no claim rows. The runner bodies call them: `runTests` calls `executeTest`
  (`prelude:1470`), `runPCTTests` calls `executePCTTest` (`:1518`), and `runPCTTestsFromPath` calls
  `loadPCTManifest` (`:1550`). Any call therefore type-checks and then fails at lowering.
- **Reachability.** `meta::pure::test` is not a core import (`NameResolver.java:213-245`), so they are reachable
  only by FQN or an explicit import.
- **The PCT program does not need them from the prelude.** Channel A types none of them on our side. Channel B
  already loads `pct_core.pure` and `surveyor.pure` as files of its own model tree; today the prelude copies shadow
  those (`Compiler.java:327-348`).
- **Which world would want them.** Only a program that runs Pure tests through the surveyor would want them, and no
  such program exists in this repo. Under T2 such a program would bring them in by file
  (`docs/PRELUDE_MODULE_HOMEWORK_2026_09_08.md:87-89`), along with implementations of the three upstream natives.
- **The project's recorded stance:** "treat surveyor itself as a later option, not the plan" and "Do not plan on
  running `surveyor.pure` verbatim" (`docs/PCT_AUDIT.md:347, 570`). That audit is dated 2026-08-04 (`df0c83c2c`),
  so some of its facts predate later batches.

**`TestedByResult`: yes, as long as `Extension` is carried verbatim.**

- Removing it alone makes `checkClosed` throw (`PG:936-979`).
- Its package `meta::pure::functions::test` is a core import (`NameResolver.java:236`), so it is visible bare.
- It is engine main code in a test-support package. It is not test code in the sense of the other 18.

### If the 18 were to leave: facts to know first

- **Removing only the `excluded()` exemption breaks the generator.** The bodied surveyor functions would still be
  carried (the plural-only filter) and would name `TestReport`, `TestResult` and `TestGroup`, so the module resolve
  check (`PG:763-773`) would throw. The function filter (`PG:1540`) and the `native function` slice (`PG:1506`) must treat
  `meta::pure::test::` as test support too.
- **Channel B (OPEN).** No longer shadowed, the two files would load from the platform tree with their `native function` declarations
  dropped. Whether that adds a model wall against the shrink-only ceiling `walls.size() <= 20`
  (`ChannelBEssentialTest.java:50`) is unknown. To settle it, run the channel B essential suite on a branch without
  the names.
- **Reference lane (OPEN).** `runTests`, `runPCTTests` and `runPCTTestsFromPath` would lose the declarations of the upstream natives
  they call (`OurResolutions.java:69-70`) and join the failed bodies, so the golden would move. To settle it, run
  `//spec:reference_lane_report`. A deliberate change is re-blessed with `bazel run //spec:update_reference_lane`
  (`spec/src/test/java/com/legend/generators/ReferenceLaneReport.java:20-22`).
