# Research line 3: the plan's steps against our code (worktree at 6e99cef11)

Read-only audit, 2026-09-26. All paths under `core/src/main/java/com/legend/` unless noted; tests
under `core/src/test/java/com/legend/`. Feeds `PLAN_AUDIT_2026_09_26.md` §2 and §7.

## #47 — kernel chooses like the reference
- CONFIRMED exists: `compiler/spec/InferenceKernel.java` (2049 lines): `resolveOverload` :1068/:1078-1215,
  `score` :1248, `paramTypeScore` :1322, `paramMultScore` :1479, `mostSpecific` :2002, `moreSpecific`
  :2025, `allSameShape` :1133, native-over-module :1147, `unify`/`unifyMult` present (:764 cites
  MultiplicityMatch, :809 covariant containment).
- UNSAID: `scoreNonLambda` :1230 (public) is a SECOND scorer, used by `Typer.selectRankedByPresentArgs`
  :2133/:2152 to pre-pick overloads from non-lambda args before lambdas are typed (`checkWithDeferred`
  :1893-1922). If `score` becomes a Match ordering and `scoreNonLambda` stays a sum, the two disagree.
- UNSAID: `nearestInLinearization` (:1187) and the Nil bottom-value tie-break (:1161-1183).
- TRAP (order): a catalog native and a same-signature bodied twin BOTH enter the candidate set
  (`FunctionCompiler.functionsAt` :62 natives + :69 `addModelOverloads`, unless suppressed); the tie
  is resolved only by `allSameShape` → first wins (:1133-1139) and the distinct-FQN same-shape case
  (`mapping::execute` vs `router::execute`, :1125-1131). Deleting those before A1 merges twins by id
  yields "Too many matches" on every such call. #47 and A1 are coupled.

## A1 — candidate set as declarations
- CONFIRMED: `NameResolver.resolveCallCandidates` :362-381 (wildcards + own package + 32 `CORE_IMPORTS`
  :213 → `addKnown` = string concat + `HashSet.contains` on `scope.knownFqns()`); universe built per
  resolve by `knownFqns()` :387-397 and ALREADY contains every parsed function's
  `SignatureMangle.mangle(f)` id (:391-393) plus `PLATFORM_FQNS`. Ids-as-strings are computable at
  phase D.
- REFUTED (layering): `FunctionId` is `platform/FunctionId.java`, imports `com.legend.model.Function`/
  `SignatureMangle` (:6-7); `AppliedFunction` is `protocol/spec`. `ArchitectureTest:400
  protocolIsTheBottomLayer`, `:347 modelIsPureData`, `:171 packageDependenciesAreAcyclic`.
  protocol→platform→model→protocol is a cycle.
- CONFIRMED hidden readers of `candidateFqns` (10 files): `parser/OperatorParts.java` :67,:123,:136,
  `lineage/PkInference.java` :98-99, `compiler/ResolvedNames.java` :28, `validation/ValidateDesugar.java`
  :283, `compiler/StatementInline.java` :203, `compiler/spec/SortChecker.java` :184,
  `compiler/spec/CallShapes.java` :87, `Typer` :2531,:2537,:2542, `NameResolver` :1708-1743,
  `NameResolutionContractTest` (4).
- CONFIRMED: `DeclarationTable` (`platform/DeclarationTable.java` :50 `of`, :72 `get(id)`, :77 `at(fqn)`)
  is built lazily at `PureModelContext:584-591` over `Pure.all()` + `model.functions()` — phase F.
  `FunctionCompiler` is constructed with only `ModelBuilder` (:26-33); no table handle.
- TRAP: `DeclarationTable.of` keeps the BODIED twin for an id (:60-65). `FunctionCompiler.compile`
  sets `isNative = f instanceof NativeFunctionDefinition` (:186). After A1 every native the world also
  declares with a body becomes `isNative=false`: 13 `.isNative()` readers in main, incl.
  `Typer.requiresNormalization` :1674 (stereotype string "NormalizeRequiredFunction" :1677) and
  `StatementInline` :33-34. "The twin's body is never chosen" holds only where the row is
  Intrinsic/Form (`CallNodes.mint` :47 `runsByRule`); the 191 missing overloads at 77 FQNs get `Body`.
- CONFIRMED deletions and pins: `PlatformTypes.isPlatformOwnedFunction` :690-696 (23-entry list + 4)
  ONE caller `FunctionCompiler:68`; `PCT_PROFILE` caller `FunctionCompiler:102`; `SUPPRESSED_ONCE`
  :70,:97 also in `ArchitectureTest:946` mutable-field allowlist. Tests pinning the deleted behaviour:
  `compiler/PctFunctionSuppressionTest.java`, `compiler/BareNamesTest.java`,
  `builtin/NativeCatalogGovernanceTest.java` :152,:165, `NameResolutionContractTest`.
  FUNCTION_CATEGORY_CHECK pin (IdentityGuardrailTest:137 = 13): A1 removes 2 → 11; the rest are
  `isVerdictFunction` (Compiler:954, StatementInline:192,:277), `isStatementOnly`,
  `"NormalizeRequiredFunction"`, `"_this"`.
- CONFIRMED grammar contradiction: corpus parsed with `Dialect.LEGEND_PLATFORM`
  (`spec/.../rcorpus/MinimalCorpus.java` :256,:274), queries with `LEGEND_LITE` (`Compiler.java:515`,
  server `LegendHttpServer:231` → `Compiler.plan`, `wasm/src/main/java/planner/Wasm.java:93`). NOTHING
  records the grammar on the tree: `ParsedModel` (`model/ParsedModel.java:22-27`) has no dialect;
  `NameResolver.Scope` has only `boolean prelude` (:2004-2011). Today the handler surface (BareNames
  tier 1 `EngineHandlers.fqnsOf`) serves EVERY bare call at the typer (`FunctionCompiler:42`) and the
  resolver's captured merge for any prelude scope (`NameResolver:1719-1725`; `buildModel`/`buildModule`
  both resolve with preludeOn=true :176,:188). Which tier serves the 223 bare names is UNPROVEN.
- CONFIRMED tolerant modes everywhere the gate runs: `NameResolver.resolve(parsed, wallSink)`
  :168-176; `Compiler.buildModule` :425; corpus loader `MinimalCorpus:293`; `FunctionCompiler.compileAll`
  DROP-AT-OVERLOAD :117-133; `Typer.candidatesOf` :2536-2557 swallows broken import candidates.
- Own-package tier: `NameResolver:252` cites "§2.4b of the resolution audit: own package always
  visible bare" — the reference's code says no such tier.
- The un-indexed work: `ResolvedNames.referents` (:26-33) calls `BareNames.catalog(name)` — rebuilds
  `EngineHandlers.fqnsOf` + 32 concats + `CoreFn.parseNames` + `removeIf` + `Pure.nativeFunctionsAt`
  per FQN — on EVERY `ResolvedNames.names(af, X)` check: 33 call sites in 17 files (Typer 5,
  MappingNormalizer 4, ValidateDesugar 3, ContextReading 3, FoldChecker 3, …). Unmeasured.

## #46 — corpus drift
- REFUTED as stated. Receipts `~/legend/platform-architecture/receipts/untangle-4b/corpus-curve-{duckdb,h2}.txt`:
  start 7be92f9cf passes 32/37 (jvm 33.7/38.8s); e2f201210 35/38; 639063f9a 36/40; c5caddd3b 158/151
  (the `rewriteSwitch` runaway); 52e4acf6f 32/37 (jvm 36.6/42.4s). H2: 19/55 → 22/57 → 132/185 →
  20/56. Per-PASS time at 52e4acf6f is at program start; the residual +2.9s/+3.6s is JVM BOOT
  (GATES.md 4b.1 entry: "boot ~1.8 → ~4.8s per JVM … the resolver's universe and the larger
  prelude"). No jstack/profile of resolution exists. Load avg 2.66–5.74 in the receipt headers.

## A2 — chosen declaration through lowering
- CONFIRMED: `TypedNativeCall.callee: TypedFunction` (`compiler/spec/typed/TypedNativeCall.java:27`),
  `TypedFunction.definition` (`compiler/element/TypedFunction.java:60`, `@Nullable`; test ctor :78-84
  leaves it null; 3 main-code `new TypedFunction(` sites: ObjectReferenceArms, SignatureApart,
  FunctionCompiler — verify the first two pass a definition).
- REFUTED "registered under the declaration id": registries keyed by `Function.signatureKey()`
  (`model/Function.java:51-62`), NOT `FunctionId`. `ImplementationTable:66-69` translates via the
  catalog. Readers of `signatureKey()`: 12 in lowering (`Scalars:318,:1041,:2552-2557`,
  `Lowerer:1215,:3410`), 81 elsewhere in main (StaticFold inlining stack :294, Typer normalizing
  stack, UserCallInliner…).
- CONFIRMED 158 `nativeKeysAt` registrations: Scalars 120, DateShifts 7, Aggregates 6, JsonLane 5,
  CollectionLanes 4, Windows 3, Coercions 3, ListRules/ListEncodings/AsorReaders 2 each,
  StringPredicates/ScalarStats/LambdaBinding/FeatureRules 1 each. Indexes `Pure.java:600-602,:626-636`;
  `nativeKeysAt` :698/:713/:729; `nativeNamed` :772 (used `Lowerer:3410`); `registeredAt` :815.
  `ArchitectureTest` allowlists `Pure$Index` fields (~:940).
- UNSAID: phase H (`resolver/`) re-looks functions up by FQN string: `Callees` 7, `TemporalFrame` 6,
  `AssociationJoins` 6, `SyntheticHeads`/`GraphEmission`/`ClassSources`/`ChainDispatch` 3 each,
  `JsonSourceFrame`/`ChainNormalizer` 2 = 35 sites.
- A2 does NOT depend on A1: keying by `FunctionId.of(callee.definition())` works today.

## A3 — mints
- CONFIRMED 143 by file: Typer 30, RelOpTranslator 20, MappingNormalizer 19, SpecParser 16,
  ViewRelation 7, LambdaBodies 7, JoinChecker 6, ValidateDesugar 5, JoinChainEmission 5, JsonChecker
  4, CallShapes 4, … OperatorParts 1.
- REFUTED gate "143 → 0": the pin regex (`IdentityGuardrailTest:75` `new AppliedFunction\(\s*"`)
  counts SpecParser's 16 + OperatorParts' 1 (parser files excluded only for LOCAL shapes, :155-159);
  and `new AppliedFunction(Pure.IF__….qualifiedName(), …)` passes the pin while still putting a
  spelling on the node.
- UNSAID: 53 mints are in the normalizer (phase E, before F): no `TypedFunction`/table of model
  declarations exists there — only the static catalog table (`PlatformRegistrations:46-47`). The one
  typed mint is `compiler/spec/CallNodes.mint` :29-50.

## A4 — element/member binding
- CONFIRMED contradiction with invariant 5: `Type.ClassType(String fqn)` (`compiler/element/type/
  Type.java:256`; header :15-17), `TypedClass.superClassFqns: List<String>` (:30). An element's FQN
  IS its identity (no overloads), so many of the 175 compares are legitimate identity checks.
- Member binding already type-directed (`Typer:548-557` `ctx.findProperty` on the receiver's class).

## B — forms by declaration
- CONFIRMED dispatch today: `Typer:560` `CoreFn.of(af.function())` → `:566 ReceiverOwnedFunctions.of`
  → `:571 applyCore` (`:1308-1454`, exhaustive switch over 65 `CoreFn` arms; 39 `*Checker.java`).
  BEFORE any argument is typed; each checker owns its argument typing. `CoreFn.OWNS`
  (`platform/CoreFn.java:215-390`) maps forms → FQNs; `ImplementationTable:118-131` gives one `Form`
  row per id; `CoreFn.of(qualified)` consults `OWNER_OF` (:420).
- REFUTED "dispatched by the chosen declaration": the overload cannot be chosen before a form's
  lambda args are typed (`checkWithDeferred` :1893-1922 is the generic version). Feasible: dispatch
  when any CANDIDATE id (A1) has a `Form` row. Needs A1, not A2. Traps: candidate sets spanning two
  rules (`filter` → JsonChecker vs FilterChecker :1446-1451; `sort`, `map` collection vs relation),
  `NEW` owns no FQN, `INTERNAL_DESUGAR` guard (`CoreFn:410-414`).
- UNSAID: `agg` is UNOWNED (`GroupByChecker` matches `"agg"`; not in CoreFn); upstream declares it
  as a BODIED function in engine `core`.
- 21 sites: Typer 7, SortChecker 4, ScanRelations 2 (lineage), SourceSubst 2, MatchChecker 2,
  MappingNormalizer 1 (phase E), ProjectChecker, GraphFetchChecker, DeferredArgs.

## C — one evaluator
- CONFIRMED the four: `StatementInline.rewrite` (`Compiler:777`, 291 lines, pre-typing),
  `Typer.inlineNormalized` :1672 (gate `requiresNormalization` :1674-1680 = stereotype STRING),
  `UserCallInliner.inlineBody` (`Compiler:517`, 1539 lines), `StaticFold.evalUserCall` :276-300
  (+ `eval`/`fold`, 848 lines, `FoldOp` :402-436 = 27 ops incl. PLUS/MINUS/INDEX_OF/JOIN_STRINGS/
  CONTAINS/EQUAL/IF).
- UNSAID other folders/rewriters: `LiteralFold.java` (60; the ONLY one registered, PERMANENT,
  `JavaEvalLedgerTest:50`), `compiler/spec/NormalizeFolds` (116; `CallNodes:49`), `LiteralUnroll`
  (684), `SourceSubst` (262, `inlineLets`), `LiteralMapUnroll` (`Compiler:787`), `ValidateDesugar`
  (456, `Compiler:781`), `resolver/LiteralFolds`, lowering `Fold.java` (1375, fold-vs-isolate),
  `MatchFold` (124).
- CONFIRMED tenet conflict: `grep -c StaticFold JavaEvalLedgerTest.java` = 0 — an UNREGISTERED Java
  evaluator inside the query compiler (only `ErrorShapeGuardrailTest:128` pins its file). Needs a
  TENET_CHARTER/JAVA_EVICTION_PLAN §1 ruling and a ledger row before C starts.
- UNSAID dependency: "over typed terms" requires normalise-required bodies to type standalone;
  today they cannot (`Typer:1600-1605`). C needs D first for TDS-erased shapes.
- CONFIRMED: `StaticFold` mutual-recursion bug via `inlining` stack keyed by `signatureKey` :294.

## D — kernel
- CONFIRMED 164: `manifest-census-core_relational.txt:3` `kernel=164` (also overload=160,
  unknown-function=409, unknown-type=131, other=515, normalize=42). Top messages: "T bound to
  Class<Any> vs …" (53/42/19/7), unbound T (11).
- UNSAID: no inventory of which of the 39 checkers exist only because the kernel could not type.
- TRAP: `CodeShapeGuardrailTest:38 FILE_LIMIT = 3500`, `FILE_ALLOWLIST` empty (:19). Typer 3489,
  Lowerer 3494, Scalars 3476.

## E — one ownership registry
- CONFIRMED the claims ledger is TEST scope in spec: `spec/src/test/java/com/legend/claims/{Claims.java
  171, ClaimsGenerator.java 203, ClaimRegistryTest}`; `native-claims.tsv` consumed by `PreludeGenerator`,
  `NativesGenerator`, `NativeSignatureGeneratorTest`. Mechanisms: `Registrations` record
  (`platform/Registrations.java:39-48`), `WalledBodies` (117), `Pure.LITE_SURFACE` :559,
  `INTERNAL_DESUGAR` :523, `userResolvableFunctionFqns` :755, `EngineHandlers` (97, generated),
  `PlatformTypes.isVerdictFunction` :636 / `isStatementOnly` :738, `PLATFORM_OWNED_FUNCTIONS`.

## F — load by manifest
- CONFIRMED: `UpstreamFiles.LIBRARY_FILES/SHAPE_FILES`, `MinimalCorpus`, `ManifestWorldCensusTest`
  in spec test; walls 32. Corpus loader is tolerant (`buildModule`) and LEGEND_PLATFORM.
- CONFIRMED contradiction: REAL_PLAN "E and F alongside from B" vs charter D7(c) "one variable at a
  time".

## G — packages
- `ArchitectureTest:171` checks cycles across TOP-LEVEL slices only; the cycle inside `compiler.*` is
  invisible to it. Task #6.

## Traps (implementation)
1. `FunctionId` cannot go on `AppliedFunction` (package cycle).
2. `DeclarationTable` prefers the bodied twin → `isNative=false` cascade (13 readers).
3. #47 without `allSameShape` before A1 = "Too many matches" on twins/aliases; `scoreNonLambda` is a
   second scorer.
4. `ResolvedNames.names` (33 sites) recomputes `BareNames.catalog` per check — the real hot spot
   candidate; unmeasured.
5. Removing BareNames tier 1 for "Pure source" changes corpus resolution; measure tier per bare
   name with the probe first.
6. Size guard headroom: 11/6/24 lines.
7. Pins retire with their tests: `PctFunctionSuppressionTest`, `BareNamesTest`,
   `NativeCatalogGovernanceTest`, `NameResolutionContractTest`, `ArchitectureTest:946` allowlist,
   `IdentityGuardrailTest` regexes (MINT_BY_NAME counts the parser).
8. Two identities (`signatureKey` vs `FunctionId`) — 93 `signatureKey()` readers.
9. Phase H's 35 string lookups; phase E's 53 mints and 1 `CoreFn.of` with no model.
10. The evaluator needs a tenet ruling and a ledger row.

## Corrected order (evidence-driven)
A2 first (mechanical, independent of A1) → #47 + A1 as ONE slice (twins merged by id and the tie
tolerance deleted in the same commit; probe the BareNames tier per bare name before removing tier 1;
keep walls) → a G-lite split of Typer along `applyCore`/checkers → B (dispatch on a candidate's Form
row; declare `agg`) → D → C only after a charter ruling and after D types TDS-erased bodies → E → F
(names held constant first, per D7(c)) → G rest. #46 per-pass is closed by the receipts; boot growth
belongs to F.
