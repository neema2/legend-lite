# Research line 2: how legend-engine (4.145.0) resolves function calls in engine input

Read-only audit of the pinned tree, 2026-09-26. Feeds `PLAN_AUDIT_2026_09_26.md` §1 rows 7–8.

Paths: `ENGINE` = `$(bazel info output_base)/external/+http_archive+legend_engine_src`;
`CMP` = `ENGINE/legend-engine-core/legend-engine-core-base/legend-engine-core-language-pure/
legend-engine-language-pure-compiler/src/main/java/org/finos/legend/engine/language/pure/compiler/toPureGraph`
(note: under `legend-engine-core-base`, not `legend-engine-core-pure`).

## Findings

**1. "Handler registry FIRST, then the graph" — REFUTED; the real rule is narrower.**
Trace: `ValueSpecificationBuilder.java:932-993` visit(AppliedFunction) → :954
`pureModel.buildNameForAppliedFunction` (`PureModel.java:1635-1646`: only prepends a package prefix)
→ :955 `CompileContext.buildFunctionExpression` (`CompileContext.java:476-485`) →
`Handlers.buildFunctionExpression` (`Handlers.java:3127-3140`) → `CompileContext.resolveFunctionBuilder`
(`CompileContext.java:544-568`).
`resolveFunctionBuilder`: (i) `extractMetaFunctionName` (:570-583) strips `meta::...::` when the
package is one the registry has seen (`Handlers.java:84, :3153-3163 registeredMetaPackages`), so
`meta::pure::functions::math::max(...)` becomes bare `max`; (ii) `functionHandlerMap.containsKey(name)`
exact hit; (iii) else `searchImports(name, functionHandlerMap::get)` (:585-635) tries
`<import>::<name>` for every import (META_IMPORTS :90-123, 32 packages, plus the element's section
imports via `Builder.withSection` :169-177) AGAINST THE SAME HANDLER MAP; 0 hits → "Function does not
exist" (VSB:969); >1 hits → "multiple matches" (:563).
The graph is never consulted for calls (`CompileContext.java:365-368` is for function pointers).
User-defined functions are IN the handler map: `FunctionCompilerExtension.java:79-118` registers a
`UserDefinedFunctionHandler` keyed by `func._functionName()` = `package::name` without signature
(:87-88), dispatch = `HelperModelBuilder.checkCompatibility` per parameter (:97-118). A bare user
function resolves ONLY if its package is in META_IMPORTS or the section's imports. Qualified meta
names collapse to bare, so the spelled package does NOT restrict the candidate set (Pure does).
Consequence: the "handler surface" is not a tier on top of the graph; it IS the engine's whole
function namespace (platform ids by bare name, user ids by full path).

**2. Overload choice for a handler-registered name — CONFIRMED "first dispatch test that passes, in
registration order", with two pre-filters.**
- `Handlers.java` map: bare name → FunctionExpressionBuilder (:1439). Same-arity handlers grouped in
  a `MultiHandlerFunctionExpressionBuilder`; different arities in a `CompositeFunctionExpressionBuilder`
  (:3292-3312 insertInMap, :3376-3392 addFunctionHandler). Composite iterates sub-builders in
  registration order, first non-null wins. MultiHandler: size match AND at least one handler passing
  `FunctionExpressionBuilder.test` (protocol shape: param count + lambda-vs-non-lambda per parameter)
  BEFORE compiling args; then compiles args and picks `handlers.stream().filter(h ->
  h.getDispatch().shouldSelect(params)).findFirst()`. `grp(...)` groups
  (`UnifiedInferenceFunctionExpressionBuilder.java`, 45 uses) run a ParametersInference first.
- Dispatch predicates: inline lambdas in `h(...)` are REPLACED at registration by generated ones
  (`Handlers.java:3324-3331`); `CoreCompilerExtension.java:59` extends the build-generated
  `FunctionDispatchExtension` (pom.xml:66-80 runs MatchGenerator over
  `src/main/resources/handlers_dispatch_functions.txt`, 811 ids) plus 32 calendar overrides that are
  `ps -> true` (:146-182). Generated check (`legend-engine-pure-code-compiled-core/src/main/resources/
  core/legend/compiler/matchGenerator.pure:70-170`): `ps.size()==N` && per parameter: multiplicity
  class only (isOne / matchZeroOne / matchOneMany / nothing for [*]) && type: NO check for type
  parameters or Any, else `cov_T.contains(rawType.name)` where cov_T = T plus all its specialisations
  at generation time (`Handlers.java:3455-3503`; `Nil` always passes). No distance scoring, no tie
  error, no type-argument matching, no generics unification.
- Examples: `isEmpty` (:1685-1686): [0..1] or [1] → `isEmpty_Any_$0_1$`; [*] → `isEmpty_Any_MANY`.
  Same as Pure. `max` (:2855-2873): max(Integer[1], Float[1]) → Number/Number. Same. `plus`
  (:2915-2919): `plus([1, 2.5])` → `plus_Number_MANY`. Same. `between` (:2847-2849): engine registers
  only Date/Number/String ids; a DateTime argument passes cov_Date → `between_Date_$0_1$…`, whereas
  Pure picks `between_DateTime…` (the id exists: the reference differential resolved to it).
- CONFIRMED divergence class: user-defined overloads. Engine dispatch = assignability,
  first-registered wins; registration order = element first-pass order under `maybeParallel`
  (`PureModel.java:222-223, :316, :333`; `Handlers.register` synchronized :3170) → can be
  non-deterministic. `f(Number[1])` registered before `f(Integer[1])`, call `f(1)`: engine → Number;
  Pure → Integer.
- CONFIRMED divergence class: qualified engine spellings reach the same builder; the id chosen is by
  dispatch order, so the spelled package can differ from the returned id.
Consequence: ONE rule (Pure's) for everything the corpus and platform compile. A SECOND rule only if
the plan must reproduce the engine's chosen ids for engine-grammar/JSON queries. Do not merge them:
the engine rule is order-and-name-set based and cannot be expressed as a scoring.

**3. What are the engine's inputs? "The corpus is engine input" — REFUTED.**
Relational corpus harness: `ENGINE/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-
generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/test/
java/org/finos/legend/pure/code/core/relational/Test_Pure_Relational.java`:
`PureTestBuilderCompiled.getClassLoaderExecutionSupport()` + `TestCollection.collectTests("meta::relational", …)`
— legend-pure's compiled runtime over the code repositories. Handlers/ValueSpecificationBuilder are
not on that path. Pure-side resolution: FEM:159-171 `getValidPackages`; coreImport =
`system::imports::coreImport` (m3.pure:175-215, 29 packages). Engine META_IMPORTS = those 29 +
meta::pure::metamodel::variant, meta::pure::metamodel::relation, meta::pure::precisePrimitives
(`CompileContext.java:90-123`).
Inputs that DO go through Handlers: PureModelContextData JSON (Studio/SDLC/services) via
`Compiler.compile`; PureModelContextText → `PureGrammarParser.parseModel` (`ModelManager.java:179-181`);
GrammarToJson HTTP API; `LegendCompile.java:44-59`; REPL DataCube (`legend-engine-config/
legend-engine-repl/legend-engine-repl-data-cube/.../DataCubeQueryBuilder.java`, `DataCubeHelpers.java`);
SQL/TDS/dataquality APIs.

**4. ReturnInference — "62 registrations keyed by signature id" is wrong in mechanism and count.**
Every FunctionHandler carries a ReturnInference as a field (`FunctionHandler.java` constructors;
`process()` calls `returnInference.infer(vs)`); the registry map is keyed by BARE name
(`Handlers.java:1439`). 830 distinct signature ids in Handlers.java (623 `h(` + 208 `register("…")`),
plus 61 `handlers.h(` in extensions (Relational 18, DataQuality 19, ExternalFormat 10, Service 6, Json
5, DataSpace 2, Elasticsearch 1). Named return inferences: GroupBy :408, Pivot :418, Project :442,
GraphProject :455, Join :510, Extend :521, Over :549, Distinct :967 (8). Extend :521-547 concatenates
columns of `ps[0].genericType.typeArguments[0]` with the FuncColSpec/AggColSpec relation type; Join
:510-519 merges both RelationTypes; select :2340-2342 returns ps[1] ColSpec's relation type; rename
via RenameColInference :608; sort :2409 returns ps[0]'s type.
The larger engine-only machinery is ParametersInference (49 definitions, `Handlers.java:220-1436`;
45 `grp(` groups): they rewrite the PROTOCOL arguments (lambda parameter types, ColSpec generic
types, "cov_" column-type checks at :1006) before compilation — ProjectInference :1127,
GroupByInference :1145, JoinInference :872, ExtendInference :331, SortColumnInference :722,
ReduceInference :1436.

**5. Engine-only special handling — CONFIRMED.**
letFunction: VSB:936-950 pre-registers a placeholder variable; `Handlers.java:1840`. Property on
Enumeration → `extractEnumValue` (`HelperValueSpecificationBuilder.java:194-202`; :1831-1832).
Auto-map: property on a collection → `map(coll, x|x.prop)` (HelperVSB:290-316). Milestoning: date
parameters injected (HelperVSB:329-333, :404-412; VSB:935, :991; getAll ids :1844-1846). cast to
Relation types in the return inference (:1885-1893); new :1841; subType :1700; if/match/eval by
function-type return (:1739-1742, :1934-1935, :1627-1630). TDS legacy and Relation forms under the
SAME bare names (project :1498-1507, groupBy :1513-1523, sort :2412-2414, extend :1559).
ColSpec/FuncColSpec/AggColSpec typing in the builder (VSB:331-660). Qualified-meta-name collapse
(CompileContext:570-583); package-prefixing of user names (PureModel:1635-1646). No aliasing between
handler name and id last segment.

## Consequences for the plan
(a) A1_HOMEWORK §1.2 (corpus resolved by imports alone): CONFIRMED. CORE_IMPORTS is 32 vs 29:
measure whether any corpus bare name resolves only through the 3 engine-only packages.
(b) COMPILER_DESIGN §1 and §5: REFUTED; the corpus and platform are Pure source; the engine surface
applies to PMCD/grammar-text/REPL inputs only, and there it is the whole namespace, not a tier.
(c) One overload rule (Pure's) for the corpus/platform. An engine rule only behind an "engine
input" mode if plan-parity on engine-grammar queries becomes a gate; kept as a separate table.

## Top 5 engine-side risks
1. Modelling the handler surface as a bind-time tier over Pure imports: neither compiler does that;
   it admits 830 ids (169 undeclared by the platform) into Pure-source resolution.
2. Engine-input parity means hand order + name-set membership, qualified collapse, first-registered
   user overloads (parallel first pass → non-deterministic); a scoring kernel cannot reproduce it.
3. ParametersInference (49 rewrites of protocol lambdas/colspecs) is the engine's real typing
   surface for relation/TDS forms; the plan tracks only return-type inference.
4. Engine pre-match rewrites (auto-map, enum extract, milestoning, letFunction placeholder,
   TDS/Relation under one name) make "engine input" a different language; a shared binder needs
   them as an explicit engine-mode desugaring.
5. Import-group drift: engine META_IMPORTS (32) vs Pure coreImport (29); keep both generated and
   drift-tested separately.
