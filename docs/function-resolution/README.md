# Name resolution in legend-lite vs legend-engine — the complete record (2026-10-03/04)

Everything learned while designing how Studio-lite turns `.pure` files with imports into fully-qualified entity
JSON, and the function-name divergences that work exposed. Written so the fix can be picked up later **without
redoing any of the research**: every claim cites code or a probe, every probe is in `probes/`, every measured
number is in `data/`.

**Status (2026-10-04, the user's decision):** deferred. Studio v0 stores `.pure` text **without imports**
(upstream SDLC's own rule), so it needs none of this; **v1 adds imports, and this is v1's work** — plus the
full model round trip (JSON reader, model printer) for opening existing upstream projects. See
`docs/STUDIO_DESIGN_2026_10_02.md` (S5, S14, S20).

Sources read: legend-engine `230c159196d` (`~/legend/legend-engine`), legend-studio `821c74c`, legend-sdlc
`1021fda`, legend-pure (`~/legend/legend-pure`), legend-lite at `studio` (rebased on main 2026-10-03).
Oracle: legend-engine 4.145.0 server (`~/legend/engine-dist-4.145.0`, `:6300`, started with
`java -cp "$E/lib/*" org.finos.legend.engine.server.Server server $E/config.json`); lite server
(`bazel-bin/core/server_deploy.jar 18777`).

---

## 1. Why this came up

1. Design S5/S13: models are stored as **one `.pure` file per element, exactly as typed** — imports and comments
   kept (upstream loses both).
2. Everything that *reads* models speaks upstream JSON — Depot's entities and `pureModelContextData`, the engine's
   alloy pointer, Query, upstream Studio — and that JSON must be **fully qualified**: an entity carries no imports.
3. So a file like
   ```
   import demo::shared::*;
   Class demo::app::Order { customer: Customer[1]; }
   ```
   must be served as JSON whose property type is `demo::shared::Customer`. That is **piece (a)**.
4. lite already resolves `Customer` → `demo::shared::Customer` — but only inside the compiler, after the parsed
   records are converted to the compiler's model, which cannot be emitted as JSON.
5. Designing (a) required knowing exactly how upstream resolves names — types *and* function calls — which
   exposed five places where lite accepts code legend-engine refuses.

---

## 2. Facts about upstream

### 2.1 The engine's `grammarToJson` does not resolve names

Probe (2026-10-03): the same text through both servers'
`POST /api/pure/v1/grammar/grammarToJson/model?returnSourceInformation=false`:

```
###Pure
import demo::shared::*;
// a comment
Class demo::app::Order
{
  customer: Customer[1];
}
```

Engine and lite answers are **byte-identical**. The property's type is `"fullPath":"Customer"` (as written); the
import lives only in a separate element, `{"_type":"sectionIndex","name":"SectionIndex","package":"__internal__",
"sections":[…{"_type":"importAware","elements":["demo::app::Order"],"imports":["demo::shared"],"parserName":"Pure"}]}`;
the comment is gone. Resolution happens later, in the engine's **compiler**, using the `SectionIndex` imports.

### 2.2 Who writes fully-qualified entity JSON: upstream Studio, in the browser

- Studio's graph builder resolves element paths client-side and serializes full paths; it drops the
  `SectionIndex`: `V1_PureGraphManager.ts:1935-1939` (dropped by `pureCodeToEntities` unless
  `TEMPORARY__keepSectionIndex`), `:1008-1016` (deleted from the built graph unless `TEMPORARY__preserveSectionIndex`,
  comment "we write (serialize) only resolved paths") — legend-graph, legend-studio `821c74c`.
- Inside lambdas Studio rewrites **element references only** — class paths, `PackageableElementPtr`, enum values,
  generic types — and **never the function name of an `AppliedFunction`**:
  `legend-graph/src/graph-manager/protocol/pure/v1/transformation/pureGraph/to/helpers/V1_ValueSpecificationPathResolver.ts:128-131`
  (`visit_AppliedFunction` only visits parameters), `:145-160` (`PackageableElementPtr`), `:185-197` (enum values).
- It does that only while `enableRawLambdaAutoPathResolution` holds (`V1_GraphBuilderContext.ts:185-199`): main
  graph root and `!TEMPORARY__preserveSectionIndex`. Its comment: "When we fully support section index, we would
  certainly need to revise the usefullness of this flag" — finos/legend-studio issue #1067.
- So in upstream, a user can **not** use imports in Studio (text mode loses them; every name must be fully
  qualified), and a call's function name in saved JSON is whatever was typed.

### 2.3 upstream SDLC refuses imports in `.pure` files

`legend-sdlc/legend-sdlc-protocol-pure/src/main/java/org/finos/legend/sdlc/protocol/pure/v1/PureEntitySerializer.java:244-260`:
`validateSectionIndex` throws "Imports in Pure files are not currently supported" for any `ImportAwareCodeSection`
with imports. Its `canSerialize` (`:84-103`) only checks that print→parse does not throw (not that it round-trips
exactly). legend-sdlc stores `.pure` per element (`ProjectStructureV13Factory.java:67` serializers `pure` then
`legend`; `ProjectStructure.java:176` first that `canSerialize`) and serves JSON parsed from them.

### 2.4 The engine's function-name rule (read in code)

legend-engine-language-pure-compiler, `toPureGraph/CompileContext.java`:

- `resolveFunctionBuilder(functionName, metaPackages, functionHandlerMap, …)` (`:544-571`):
  1. `extractMetaFunctionName` (`:573-586`): if the name contains `meta::` and its package is in
     `registeredMetaPackages`, reduce it to the short name.
  2. `functionHandlerMap.get(extractedName)` — if present, that builder.
  3. else `searchImports(extractedName, functionHandlerMap::get)` (`:588-633`): for each import package
     (`this.imports`), try `package + "::" + name`; collect hits; **0 → null** (later "Function does not exist"),
     **1 → it**, **>1 → `EngineException("Can't resolve the builder for function '<f>' - multiple matches found
     [a, b]")`**. A cache (`SEARCH_IMPORTS_CACHE`) remembers single hits.
- `this.imports` = `META_IMPORTS` (`:90-121`, the 32 packages in `data/engine-meta-imports.txt`, "taken from m3.pure
  in PURE") plus the element's section imports (`Builder.withSection`, `:169-177`: "we add auto-imports regardless
  the type of the section or whether if there is any section at all").

`toPureGraph/handlers/Handlers.java`:

- Constructor `:1450-1490` calls `registerFunctionDispatch`, `registerMathBitwise`, `registerMathInequalities`,
  `registerMaxMin`, `registerAlgebra`, `registerOlapMath`, `registerAggregations`, `registerStdDeviations`,
  `registerVariance`, `registerCovariance`, `registerTrigo`, `registerStrings`, `registerDates`, `registerTDS`,
  `registerJson`, `registerRuntimeHelper`, `registerAsserts`, `registerUnitFunctions`, `registerCalendarFunctions`,
  then many `register(grp(...))` / `register(h(...))`.
- **Platform functions are registered under their SHORT name**: `h("meta::pure::functions::collection::filter_T_MANY__Function_1__T_MANY_", "filter", true, …)`.
  Several platform functions share one short name and one builder — relation, TDS and collection `filter` are all
  under `"filter"` (`:1488-1496`), the argument types choosing (`MultiHandlerFunctionExpressionBuilder` /
  `UnifiedInferenceFunctionExpressionBuilder`).
- Some names are registered through **lists**, not `h(...)`: `:292` (`"greaterThanAny", "greaterThanAll",
  "greaterThanEqualAny", "greaterThanEqualAll", …` — the relation quantifier family). A source scan for
  `h(`/`register(` misses them (§5.3).
- Compiler **extensions** add handlers (`:2025-2032`: `getExtraFunctionExpressionBuilderRegistrationInfoCollectors`,
  `getExtraFunctionHandlerRegistrationInfoCollectors`, `getExtraFunctionHandlerDispatchBuilderInfoCollectors`) —
  found in data quality, relational, external format, service, JSON, data space and Elasticsearch modules.
- `registerMetaPackage` (`:3153-3163`) records the package of every registered handler under `meta::`, which is
  what step 1 strips.
- **User functions are registered under their FULL path**: `FunctionCompilerExtension.java:97`
  (`context.pureModel.handlers.register(new UserDefinedFunctionHandler(context.pureModel, functionFullName, …))`),
  `Handlers.register(UserDefinedFunctionHandler)` `:3176-3200` (`map.put(functionName, …)` keyed by the full name).
- The call site: `Handlers.java:3139` (`resolveFunctionBuilder(functionName, this.registeredMetaPackages, this.map, …)`).

Consequences:
- a **platform short name always wins**; an imported user function sharing it is unreachable by short name;
- a user function is reached by **full path**, or by short name **through an import** when no platform function
  has that name; exactly one import may match;
- there is **no own-package tier**;
- for user code, "platform" is **exactly the handler registry** — a platform function written in Pure without a
  handler is not callable at all, by short name or full path;
- the platform union (several platform functions under one short name) is preserved;
- JSON without a `SectionIndex` (Studio's) resolves against the 32 auto-imports only.

### 2.5 legend-pure's rule, and upstream's two compilers

- lite's resolver follows legend-pure: `NameResolver.java:1743-1752` — "several imported packages defining the name
  is NOT an error — the candidates travel on the node and the Typer unions their overloads (real pure's function
  matching collects across imports; signature picks). The platform prelude JOINS the union rather than being
  shadowed: real pure has no user/platform tiering for function matching — legend-pure's platform schema(db, name)
  coexists with core_relational's relation:: schema(rel) and the call's shape picks". Its own-package tier is
  commented (`:379-387`) "the reference has no such tier (FEM:167-168)".
- Upstream never runs user and platform code through one compiler:
  - legend-engine's Pure libraries are compiled by **legend-pure's** compiler at build time, e.g.
    `legend-engine-xts-relationalStore/…/legend-engine-xt-relationalStore-core-pure/pom.xml:59-111`
    (`legend-pure-maven-compiler`; goals `build-pure-jar`, `build-pure-compiled-jar`);
  - their tests run on legend-pure's runtime, e.g.
    `Test_Pure_Relational_ConnectionEquality.java:21-29`
    (`PureTestBuilderCompiled.buildSuite(... "meta::relational::tests::connEquality" ...)`);
  - only user code (protocol) reaches the engine's compiler and its handler registry.
- Natives: legend-pure declares 225 `native function`s, legend-engine 160 (e.g. relation `filter`/`join` in
  `core_functions_relation/relation/functions/…`); the engine's protocol has **no** native function element; user
  models can only call natives. lite: the standard library natives are a Java registry (`builtin/NativeFn`,
  `Pure.java`; `prelude.pure:6630-6770` lists them as comments); lite's prelude has 68 `native` declarations and
  550 `import` lines, parsed in lite's platform dialect at boot.

---

## 3. lite's current resolver (as of `studio`, 2026-10-04)

- `core/src/main/java/com/legend/compiler/NameResolver.java`, 2,096 lines: ~80 walk methods (`resolveClass`,
  `resolveMapping`, `resolveView`, `resolveStoreSubstitution`, `resolveLambda`, …) around one rule (`resolveName`);
  85 rule-call sites.
- Contract javadoc (`:82-120`): resolve simple names to FQNs with an `ImportScope` and the known-FQN universe;
  already qualified passes through; specific import; wildcard imports: 0 → pass through, 1 → it, >1 →
  `IllegalStateException` (for types; calls differ, below).
- Entry points: `resolve(ParsedModel)` `:158`, `resolve(…, wallSink)` `:168`, `resolveAlongside` `:182` (used by
  `Compiler.buildModel`, `Compiler.java:236`, with `bootFqns()`), `resolve(model, knownFqns…)` `:196-247`,
  `resolve(ValueSpecification …)` `:524`, `resolveQuery` `:538`, `resolveQueryIn` `:555`.
- Element scopes (`:255-275`): one `Scope` per element — its own imports (`model.elementImports()`), the universe,
  **`ownPackage` = the element's package**, `preludeOn`.
- `Scope` record (`:2055-2100`): `imports, knownFqns, typeParams, ownPackage, prelude`; `Scope.of` (no prelude),
  `Scope.preludeOf` (prelude, **no own package** — the query entries).
- Call resolution: `resolveCallCandidates` (`:362-389`) collects `pkg::name` known FQNs from (1) the scope's
  wildcard imports, (2) **the own package**, (3) `CORE_IMPORTS` (`:213`) — **identical to the engine's 32
  `META_IMPORTS`** (checked by diff, `data/engine-meta-imports.txt`); falls back to `resolveNameMulti`.
  The `AppliedFunction` arm (`:1738-1792`) then, when `captured && scope.prelude()` and the name is bare, **merges
  `BareNames.catalogTiered(name)`** (the platform catalogue) into the candidates, and stores multiple candidates on
  the node (`candidateFqns`) for the Typer to union; ties break by S7's deterministic rule
  (`docs/SEMANTICS_REGISTER.md` S7).
- `PLATFORM_FQNS` / `platformFqns()` `:311-315`.
- Other callers of `NameResolver`: `ModelNormalizer` (`resolveQuery`, `resolveQueryIn`, `platformFqns`),
  `ModelContext` and `PureModelContext` (`platformFqns`), `BareNames` and `DiagramService` (`CORE`),
  `protocol/spec/TypeAnnotation`, `normalizer/MissProbe` (`resolveClass`). Only `Compiler`'s parse paths and
  `MissProbe` walk model *elements*; the rest resolve queries (already on protocol types) or read constants.
- 58 test files use `NameResolver` directly or hand-build `ParsedModel`s.

### 3.1 The pipeline around it

- `ElementParser.parseSingleElement` is a dispatch table; every `xElement()` is tagged **PROTOCOL-FIRST** or
  **STRAIGHT-TO-MODEL** (`ElementParser.java:560-568`; worklist `docs/PROTOCOL_MIGRATION_CENSUS.md`). All
  user-model kinds are protocol-first: parse to a `Protocol` record, then `FromProtocol`/`MappingFromProtocol`
  converts it **immediately, per element** (`classElement` `:665-669`, `enumElement` `:693-697`, mapping `:730-739`,
  database `:720-727`, function `:1623-1626`, association `:1340`, service `:2565-2568`, runtime `:2605-2609`, …).
  **Straight-to-model** remain only `primitiveElement` (`:1317`) and `nativeFunctionElement` (`:2499`) — both
  platform-only (the engine refuses `native` in user models). `native Class` is protocol-first via `PClass` with
  an `isNative` flag (`:671-690`) — the precedent for a native function record.
- `FromProtocol` (827 lines) drops positions on purpose (`:24-27` javadoc: model records are value types whose
  equality 111 hand-built assertions rely on).
- `Compiler.parseSources(List<ModelSource>, sink, dialect)` (`Compiler.java:139-217`): one unit per source with its
  own imports and positions, merged, duplicates reported (first wins), optional per-file parse walls.
- `ProtocolEmitter` (3,506 lines) writes `Protocol` records as engine-exact JSON; names are written with plain
  `str(b, …)` at many sites in static methods.
- Dialects: `PmcdParser` (`grammarToJson`) parses at `Dialect.LEGEND_ENGINE`; the compile paths at
  `Dialect.LEGEND_LITE`; the prelude at the platform's own level.

---

## 4. Probes (engine 4.145.0 vs lite, `POST /api/pure/v1/compilation/compile`, text model)

Scripts in `probes/` (run with both servers up; `node docs/function-resolution/probes/<file>`).

| # | Case (probe file) | Engine | lite |
|---|---|---|---|
| A | `import my::util::*;` then `'x'->myFunc()` (`a-to-f`) | OK | OK |
| A' | A as Studio-style JSON (`grammarToJson`, `sectionIndex` removed) | **refused**: "Function does not exist 'myFunc(String[1])'" | — |
| B | `myFunc()` short, no import | refused | refused ("unknown function 'myFunc'") |
| C | user `my::util::filter(String)` imported; `'x'->filter()` | platform `filter` chosen → refused ("Index 1 out of bounds") | refused ("'my::util::filter' expects a lambda argument in position 1" — the message names the user function though the platform one was applied) |
| D | `'x'->my::util::filter()` | OK | OK |
| E | `['x','y']->filter(s|$s == 'x')` | OK | OK |
| F | `…->meta::pure::functions::collection::filter(…)` | OK | OK |
| G | user `my::util::toUpper(String):Integer` imported; caller expects `Integer` (`g-h`) | platform wins → "Type error: 'String' is not a subtype of 'Integer'" | same verdict |
| H | same, caller expects `String` | OK | OK |
| I | `'x'->my::util::toUpper()` (user, `Integer`) | OK | OK |
| **J** | user `my::util::toUpper(String, Integer)` imported; `'x'->toUpper(2)` (`j-k`) | **refused**: "Can't find a match for function 'toUpper(String[1],Integer[1])'. Functions that can match if number of parameters are changed: toUpper(String[1]):String[1]" | **accepted** |
| K | `'x'->my::util::toUpper(2)` | OK | OK |
| **L** | `my::app::C->elementToPath()` (platform Pure function without a handler) (`l-m-n-o`, `p-l-q-r`) | **refused**: "Function does not exist 'elementToPath(Class<C>[1])'" | **accepted** |
| M | `my::app::C->meta::pure::functions::meta::elementToPath()` | refused ("does not exist") | — |
| N | user `my::util::elementToPath(Integer, Integer)` imported; `1->elementToPath(2)` | OK | — |
| O | user `my::util::elementToPath(PackageableElement)` imported; short | OK | — |
| **P** | `helper()` defined in the caller's own package, no import (`p-l-q-r`) | **refused**: "Function does not exist 'helper()'" | **accepted** |
| **Q** | `a::x::f()` and `b::y::f()`, both imported; `f()` | **refused**: "Can't resolve the builder for function 'f' - multiple matches found [a::x::f, b::y::f]" | **accepted** |
| **R** | `a::x::g()` and `b::y::g(String)`, both imported; `g('z')` | **refused**: "multiple matches found [b::y::g, a::x::g]" | **accepted** |
| — | `meta::pure::router::execute(1)` / `execute(1)` | refused ("does not exist") | accepted |
| — | `meta::pure::functions::relation::greaterThanAll(1, [0])` | registered ("Can't find a match …", wrong arguments) | — |

The five divergences — **J** (union across user and platform), **L** (platform functions the engine does not
expose), **P** (own-package tier), **Q** (ambiguity accepted), **R** (overloads across packages) — all go the same
way: lite accepts what legend-engine refuses. None is in `docs/SEMANTICS_REGISTER.md`.

---

## 5. The impact measurement (2026-10-04)

### 5.1 Method

A scratch probe class in `core/src/main/java/com/legend/compiler/` (reverted, never committed), hooked into
`NameResolver`'s `AppliedFunction` arm just before `String fn = matches.size() == 1 ? …` (`:1783`), guarded by an
environment flag (`--test_env=LL_FNRES=<file of engine short names>`), printing one stderr line per call that
differs from the engine's rule:

```java
// hook in NameResolver (AppliedFunction arm), only for unresolved nodes:
if (FnResEntryProbe.ON && af.candidateFqns().isEmpty()) {
    FnResEntryProbe.record(af.function(), matches, scope);
}

// FnResEntryProbe.record(name, lite, scope) -- the classification:
String own = scope.ownPackage() == null ? "<query>" : scope.ownPackage();
if (own.startsWith("meta::") || name == null || name.isEmpty()) return;          // platform elements skipped
List<String> liteKnown = lite.stream().filter(scope.knownFqns()::contains).toList();
// user(fqn) = known && !fqn.startsWith("meta::") && !NameResolver.platformFqns().contains(fqn)
if (name.contains("::")) {                       // full path
    pkg/short split; skip unless pkg starts "meta::" && !ENGINE.contains(short) && known(name) -> "L-full"
} else if (ENGINE.contains(name)) {              // a registered platform short name
    if (liteKnown has any user(m)) -> "J"
} else {                                          // engine: user functions through imports only
    eng = [pkg::name for pkg in scope.imports().wildcards() ∪ CORE_IMPORTS if user(pkg::name)]
    liteKnown empty -> skip
    eng.size()==1: liteKnown.equals(eng) ? skip : "X1"
    eng.size()==0: liteKnown.contains(own::name) ? "P" : all liteKnown non-user ? "L" : "X0"
    eng.size()>1:  "QR"
}
// line: FNRES \t kind \t name \t own \t liteKnown \t eng \t entry
// entry = first stack frame under com.legend. that is neither com.legend.compiler.* nor com.legend.Compiler
```

Validated first on a lite server running with the flag against the J/L/P/Q/R inputs: each classified correctly.
Runs: `bazel test //... -k --test_env=LL_FNRES=…` (150 targets, ~6.5 min), then the nine suites with hits.
Caveats: `StackWalker` does not compile to WebAssembly (TeaVM: "Class java.lang.StackWalker was not found"), so the
WASM-built Query/DataCube suites could not run with the entry probe (they compile user queries anyway); the
probe tripped `//core:guardrails` twice — "new debug env flag(s) [LL_FNRES] — extend the Trace switchboard instead
of adding ad-hoc getenv reads" and "string identity / function-category checks GREW (shrink-only …):
[NAME_AFFIX_TEST 55 > 51, NAME_CUTTING 108 > 106]" — a real implementation must use the Trace switchboard and
dispatch on resolved declarations, not name strings. lite also has `com.legend.builtin.DecisionProbe` (installed
under `LL_SHADOW`, the paused rebuild's untangle step 3), whose `onResolverTier` already reports own-package hits.

### 5.2 Results

Raw aggregate: `data/divergences-by-kind-name-entry.tsv` (count, kind, name, caller package, entry; 56,880 calls,
1,092 distinct rows).

- **J: 1** — `core/src/test/java/com/legend/compiler/NameResolutionContractTest.java:151-170`
  (`preludeNativesJoinCallCandidates`: "the prelude native joins the candidate set instead of being shadowed").
- **P: 0. Q/R: 0. X0/X1 (a different single function chosen): 0** — in the corpus (DuckDB and H2), the four PCT
  lanes, spec tests, stress suites and lite's own tests.
- **L: 56,880 flagged; 56,544 real** after asking the engine about each of the 1,058 distinct names (§5.3).

By caller (real L only):

| Caller (entry) | L calls | Context |
|---|---|---|
| `com.legend.test.PureTestRunner` — the reference corpus and PCT runner (via `Compiler.resolveQuery`) | 47,747 (25,939 short + 21,808 full path) | platform: upstream runs these on legend-pure |
| `com.legend.normalizer.ModelNormalizer#resolveSynthesized` — compiler-generated bodies (legacy mapping DSL translated to functions, `meta::lite::metamodel::MetamodelMapping$class$…`, `meta::legend::lite::sourceUrl`, …) | 8,377 | platform: lite's own synthesized code |
| `com.legend.server.QueryService` — reached from the PCT lanes (81+81+69 before correction) and `core_tests` (20) | 42 after correction: `meta::pure::functions::collection::find` 14, `sortByReversed` 7, `meta::pure::functions::lang::copy` 6, `meta::pure::metamodel::relation::newTDSRelationAccessor` 5, `…relation::tests::composition::testVariantColumn_functionComposition_filterValues` 4, `meta::relational::tests::csv::toCSV` 3, `…lang::tests::letFn::letWithParam` 3, `…letAsLastStatement` 3, `meta::pure::functions::meta::deactivate` 2, `…letChainedWithAnotherFunction` 2, `fromEpochValue` 1 | platform when PCT runs through it; user when `pure/v1` does — **the entry point cannot decide** |
| lite's own unit tests compiling directly (`JUnitMain`, `MetamodelQueryFunctionsTest`, `MetamodelMappingStoreTest`, `MinimalCorpus`, `AssertVerdictSpliceTest`, `ExecuteInDbTest`, `AssertErrorNativeTest`, …) | ~380 | per test |
| model elements in `core_tests` (`execute` 11, `executeInDb` 4, `router::execute` 2, `concatenateTemporalTdsQueries` 1, `preeval::preval` 1) | 19 | lite tests using router internals |

Query bodies by first non-compiler frame (all kinds, before correction): `ModelNormalizer#resolveSynthesized`
254,090 resolutions, `Compiler#resolveQuery` 228,781, `JUnitMain#main` 16,725, `Compiler#lowerParsed` 1,273,
`Compiler#compileQuery` 633, `Compiler#target` 81, `Compiler#resultType` 29, plus per-test `sqlOf` helpers.

### 5.3 The engine's reachable set must come from the engine

- A source scan of `h("meta::…", "name"` / `register("meta::…", "name"` across legend-engine (non-test `.java`)
  yields **439** short names (`data/engine-handler-names-source-scan.txt`), from the core compiler (830 matches)
  and the data quality (19), relational (18), external format (10), service (6), JSON (5), data space (2) and
  Elasticsearch (1) compiler extensions; 40 `FunctionExpressionBuilderRegistrationInfo(` and 10
  `FunctionHandlerRegistrationInfo(` constructions exist too.
- The scan **misses** list-registered names: asking the engine about every one of the 1,058 names lite flagged L
  (`probes/ask-engine-reachability.mjs`: compile `function my::probe::q(): Any[*] { <name>() }`; "Function does not
  exist" = unreachable, "Can't find a match"/success = reachable; a handler crashing on zero arguments, "Index 1
  out of bounds for length 0", also means a handler was found) gave **1,038 unreachable, 20 reachable**
  (`data/l-verdicts.tsv`): `equalAll equalAny greaterThanAll greaterThanAny greaterThanEqualAll
  greaterThanEqualAny lessThanAll lessThanAny lessThanEqualAll lessThanEqualAny` by short name and by
  `meta::pure::functions::relation::` path — registered at `Handlers.java:292`.
- Therefore the table lite needs ("which platform functions may user code call") is generated **by asking the
  pinned engine**, committed as data, re-generated on an engine upgrade.

---

## 6. Design work for piece (a) (fully-qualified entity JSON)

### 6.1 Options weighed

| Option | What | Copies of "where names appear" | Notes |
|---|---|---|---|
| 1 | a second resolver over protocol records, for the JSON path only, sharing `resolveName` | two (protocol walk + model walk) — can drift | ~resolver-sized, new |
| 1b | resolve inside `ProtocolEmitter` at each reference site | two (emitter sites + model walk) | thread a resolver through a 3,506-line static emitter |
| **2** | move resolution onto the protocol records, once: parse every file to records → resolve (the resolver ported to protocol types) → convert to the compiler's model; delete the model-level resolver | **one** | ~resolver-sized, replacing it; JSON for (a) falls out of the existing emitter |

### 6.2 What real compilers do

Java (javac Enter collects declarations, Attr resolves; `.class` files store fully-qualified names — source keeps
imports, artifacts are fully qualified, exactly our split); rustc (collect definitions, resolve once, lower the AST
to a resolved HIR); Roslyn, TypeScript, rust-analyzer (keep the source tree exactly as written, record resolution
in a side table — "this name at this position means that symbol" — shared by compiler and IDE). Common pattern:
**index declarations → resolve once → every consumer reads the result**. Option 2 is the javac/rustc shape; the
IDE-grade side table is compatible (protocol records keep positions, so each resolved name knows where it was
written — useful for go-to-definition, hover, rename later).

### 6.3 Option 2's downsides and their answers

1. **Platform natives.** Native functions and `Primitive` have no engine protocol form and are straight-to-model.
   Answer: a `native` flag on lite's own function record (as `native Class` already has), the emitter refusing to
   write natives (as the engine refuses them in user models). This is lite-internal — lite's `Protocol` records are
   lite's own hand-written mirror (`Protocol.java`, 3,194 lines), not generated from upstream, and no wire shape
   changes. Alternative: keep a tiny model-level path for the prelude (two resolvers again).
2. **The parse stops streaming.** Wildcard resolution needs every element's name, so naively all records are held
   before conversion (a memory spike at stress-corpus scale, ~400K lines). Answer: two passes — a cheap name index
   first (lite's dormant `ide/ModelIndexer` is such a scanner; the planned memoized query layer has the same
   `declIndex` step, EXECUTION_PLAN W2.2b), then parse → resolve → convert per element as a stream.
3. **Two record kinds that look identical.** `grammarToJson` must keep emitting unresolved names (byte parity);
   entity JSON emits resolved ones. Answer: resolved records as a distinct type, so passing the wrong one is a
   compile error.
4. **Tests.** 58 test files use the resolver or hand-build models; most are fully qualified; the resolver's own
   tests and short-name-dependent ones get ported.
5. **Regression risk in porting ~80 walk methods.** Answer: differential migration — run old and new side by side
   and require identical compiler models over the whole corpus (5,259 sources) and every test, before deleting the
   old one.
- Unaffected: resolution semantics and order (resolution already runs after all parsing), the eager-knowledge /
  lazy-work tenet, the other resolver users. Upside: resolution errors can carry source positions (protocol records
  keep them; the model drops them) — directly useful for W1.2 (diagnostics).

### 6.4 Reordered plan for (a) (agreed 2026-10-03)

1. Build the new resolver over protocol records **first, used only for the JSON path** — ships (a) early.
2. Run it differentially against the old resolver on the whole corpus and every test.
3. Then switch the compiler to it, do the natives housekeeping, delete the old resolver.

### 6.5 Function calls in the JSON

- Element references resolve by name alone. Function calls are different: with lite's current union, *which*
  function a call means is decided by the Typer (overload selection), so emitting the right name would need the
  Typer's binding per call site (a side table keyed by source position).
- With the engine's rule (§2.4), the name → function-family choice is purely name-based (platform short name →
  the platform family; otherwise exactly one import → that user function), and types only pick among overloads
  *within* one family, which share one name. So once lite follows the engine for user code, (a) emits:
  - a call bound to a platform function → **as written** (the engine finds the same family by short name);
  - a call bound to a user function → **its full path** (`"function":"my::util::myFunc"`), which the engine
    reaches by full path in every case of §4, including Studio-style JSON with no imports (A').
- If lite kept the union instead, a mixed platform/user candidate set would need the Typer side table — avoided by
  fixing the rule first.

---

## 7. The fix (when picked up)

### 7.1 Alternatives weighed (2026-10-04)

| | Approach | Pros | Cons |
|---|---|---|---|
| A | two rule sets selected by context | upstream-faithful | two rule sets in one resolver; mis-tagging silently changes meaning; narrows lite's lenient dialect for users (a product decision); needs the engine's name table as maintained data; removes capabilities lite users may rely on; new user-visible errors |
| B | keep lite lenient; write every call as the function lite bound (full path) in published JSON; refuse L at publish | no semantic change inside lite | the same `.pure` text means different things in lite and on the engine; Q stays silently resolved; needs the Typer side table |
| C | engine rules everywhere, one rule set | simplest | breaks ~56,500 reference-suite calls (L) — infeasible unless platform tests were compiled separately |
| **D (refined)** | one call-resolution rule everywhere — (1) if any platform function has the name: all platform functions of that name, arguments pick (legend-pure and the engine agree); (2) otherwise user functions through imports, exactly one — plus a **context that controls only visibility**: which platform functions count (user context: the engine's reachable set; platform context: all) and whether the own package counts as an import (refused in user context; unchanged in platform context) | one rule; the context can only ever affect visibility | the context must still be passed explicitly by callers |

Why D needs no re-measurement of the platform for J/Q/R: they concern **user** functions; in platform context
every function is a platform function, so step (1) always applies and the prelude keeps legend-pure's union. P and
L are the visibility differences, kept as today in platform context.

### 7.2 Context assignment (measured, §5.2)

- **Platform:** the prelude (boot), `ModelNormalizer`'s synthesized bodies, `PureTestRunner` (reference corpus and
  PCT), lite tests that deliberately use platform internals.
- **User:** `pure/v1` (`PureV1Api`), the WASM planner's exports (Query/DataCube in the browser), Studio-lite's SDLC
  and publish paths, the LSP.
- `Compiler.resolveQuery` and `QueryService` serve both → **the caller passes the context**; no default
  (AGENTS invariant 4).

### 7.3 Step by step (each its own commit, full gate before the next)

0. **Announce** in `docs/IN_FLIGHT.md` on `main`, naming the files: `compiler/NameResolver.java`,
   `Compiler.java` (entry points), `normalizer/ModelNormalizer.java`, `test/PureTestRunner`, `server/PureV1Api.java`,
   `server/QueryService.java`, `wasm/…/Wasm.java`, `compiler/NameResolutionContractTest.java` and the tests touched.
1. **Context, no behaviour change.** A context value (platform | user) on the resolver's `Scope`; every entry point
   states it explicitly. Both contexts keep today's rules; the full gate proves the plumbing changed nothing.
   Use the Trace switchboard for any diagnostics (not `getenv`).
2. **The engine's reachable set as data.** A tool asks engine 4.145.0 (the pinned oracle, `tools/oracle-pins.env`)
   whether each lite platform function (short name and full path) is reachable from user code (the method of
   `probes/ask-engine-reachability.mjs`) and writes a committed data file; a test checks it is well-formed and
   consistent with lite's catalogue; re-run on engine upgrades.
3. **Pin the engine's verdicts as tests.** §4's cases plus the quantifier family as permanent tests asserting that
   lite in user context gives the engine's verdict (compiles / "does not exist" / "multiple matches" / the right
   function). They fail here.
4. **Apply rule D in user context.** Short name in the reachable set → the platform family, arguments pick, no user
   candidates (J); otherwise user functions through the file's imports plus the 32 auto-imports, exactly one
   (Q, R → "multiple matches"; none → "does not exist"), no own package (P); a full path to an unreachable
   platform function → refused (L). Platform context unchanged. Step 3's tests pass; rewrite
   `NameResolutionContractTest.preludeNativesJoinCallCandidates` to assert the engine's rule; lite's ~380 test calls
   to internals run in platform context or are rewritten; a `docs/SEMANTICS_REGISTER.md` row records the
   platform/user split as upstream's two compilers; fix C's misleading error message (it names the user function
   while applying the platform one). Dispatch on resolved declarations, not name strings (the guardrail's
   shrink-only identity checks).
5. **Piece (a).** Element references through the new protocol-record resolver (§6.4), function calls by step 4's
   rule (§6.5); prove with the engine oracle — an element written with imports and short names, through (a), equals
   byte for byte the engine's `grammarToJson` of the same element written fully qualified without imports (minus the
   `SectionIndex` and source positions) — and that the published JSON compiles and means the same on the engine.

---

## 8. The full model round trip (also deferred; needed to open existing upstream projects, design S19)

lite today (verified 2026-10-04):

| Direction | Models | Lambdas and expressions |
|---|---|---|
| text → JSON (`grammarToJson`) | built, byte-exact with the engine (`PmcdParser`, parity on 5,259 sources) | built, byte-exact |
| JSON → text (`jsonToGrammar`) | **not built** — `PureComposer` has only `lambda` and `valueSpecification` (`PureComposer.java:64, 73`); no `jsonToGrammar/model` route | built, byte parity with upstream's printer (`ComposerParityTest`) |
| JSON → compiler (`data` model contexts) | **not built** — `PureV1Api.java:497` "the PMCD reader is not built" | built |

`parser-equivalence/…/TestGrammarRoundtrip.java` is a recorder that shadows the engine's round-trip test base to
harvest its fixtures (`FixtureRecorder.record`), not a lite round trip.

To build: (e) the model JSON reader (JSON → `Protocol` records → compiler; the mirror of `ProtocolEmitter`) and
(f) the model printer (`Protocol` records → Pure text, byte-equal to engine 4.145.0's `jsonToGrammar/model`, goldens
from the engine as for lambdas). With them: existing projects' JSON-only elements (structure v0 `/entities/**.json`,
v11–v13 `src/main/legend/*.json`) open as text; upstream clients' JSON saves are printed to `.pure` under S5's
exact-round-trip rule (refused, never a `.json` fallback, when JSON → text → JSON does not give equal records);
upstream-Depot dependencies compile; S19's acceptance test (the Legend showcase projects opened, every element
viewed, one edited and saved, every untouched element byte-identical).

---

## 9. Files in this folder

- `probes/` — the probe scripts (Node; engine on `:6300`, lite on `:18777`).
- `data/engine-meta-imports.txt` — the engine's 32 auto-imports (`CompileContext.java:90-121`), identical to lite's
  `CORE_IMPORTS`.
- `data/engine-handler-names-source-scan.txt` — 439 short names from scanning `h(`/`register(` (incomplete; §5.3).
- `data/l-names.txt`, `data/l-verdicts.tsv` — the 1,058 names lite flagged L and the engine's verdict on each.
- `data/divergences-by-kind-name-entry.tsv` — the measurement's aggregate (56,880 calls).
