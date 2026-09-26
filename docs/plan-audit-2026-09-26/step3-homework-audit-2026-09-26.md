# Audit of step3-design.md (2026-09-26) — adversarial, read-only

Tree audited: `/Users/neemsandv/legend/legend-lite/.claude/worktrees/build-audit` at `4c9c4364a`
(two commits after the design's `16d2f8ba3`; `git diff --stat 16d2f8ba3 HEAD` touches nothing under
`core/src/main/java/com/legend/{compiler,protocol,parser,normalizer,model}` or `Compiler.java`, so the
design's main-tree line numbers are checked against the same code). Paths below are relative to
`core/src/main/java/com/legend/` unless absolute. "KR" = `kernel-reading-2026-09-26.md`.

Severity: BLOCKER = the plan fails when implemented as written; WRONG = a stated fact is false;
GAP = something the plan needs and does not say; NIT = stale citation or wording.

---

## A. Bindings (3a)

### 1. BLOCKER — Identity-keyed Bindings do not survive the rewriters that run between phase D and the typer; the design names four, the tree has ~180 mint/rebuild sites

**Claim** (§1.1, §1.3): "the rewriters (`OperatorParts:67,123,136`, `SortChecker:184`, `CallShapes:87`)
rebuild nodes … a rewriter that mints a new node from an old one must RE-KEY the binding … the four
rewriters call"; §1.3 row "`SortChecker:184`, `CallShapes:87` | `bindings.rekey(af, inner)`".

**Evidence.** Every `AppliedFunction` construction after resolution produces a node the typer has never
seen. Count outside `parser/` and `protocol/` (`grep 'new AppliedFunction(\|infixRun(\|withParameters(\|asGrouped('`):
289 sites; 108 of them are in `normalizer/` (whose outputs pass through `ModelNormalizer.resolveSynthesized`
and would be bound), leaving **181 in `compiler/`, `validation/`, `lineage/`, `test/`**, by file:
`compiler/spec/Typer.java` 53 (+2 `withParameters`), `JoinChecker` 13, `SortChecker` 11,
`ProjectChecker` 8, `LambdaBodies` 7, `CallShapes` 7, `JsonChecker` 5, `StatementInline` 4,
`ValidateDesugar` 6, `IsDistinctChecker` 3, `GroupLambdaAggs` 3, `GroupByChecker` 3, `FoldChecker` 3,
`ExtendChecker` 3, `StaticFold` 2, `DistinctChecker` 2, and one each in `SourceSubst`, `AlphaRename`,
`RenameChecker`, `GetAllChecker`, `FromChecker`, `EvalChecker`, `CsvCensusChecker`, `LiteralMapUnroll`,
`ScanRelations`, `PureTestRunner`, `ServiceTestRunner`, `NameResolver`.

The structural rewriters that rebuild WHOLE SUBTREES (every call inside them becomes a new node) and
run before or during typing:
- `compiler/spec/SourceSubst.java:187` `case AppliedFunction af -> af.withParameters(…)` — called by
  `Typer:1679,1695` (`inlineNormalized`: NormalizeRequired bodies are substituted then `synth`ed),
  `Typer:1779,1789`, `Typer:2261`, `LambdaBodies:48,59`, `EvalChecker:114`, `StaticFold:253,266,724`,
  `MayExecuteChecker:33`, `GraphFetchChecker:101`, `GenerateTestDataChecker:70`, `StatementInline:114,119`,
  `LiteralMapUnroll:80,85`, `ValidateDesugar:197`.
- `compiler/spec/AlphaRename.java:39` `case AppliedFunction af2 -> af2.withParameters(…)`.
- `compiler/spec/StaticFold.java:203` `return af.withParameters(ps.stream().map(p -> fold(p, scope)).toList())`.
- `compiler/StatementInline.java:121,140,160,171` — the front-door statement-level β-reduction
  (javadoc :25-33) rebuilds lets and calls of every inlined program.
- `Typer:1710-1727` `expandFunctionValuedHelperArgs` returns `af.withParameters(np)` and is applied to
  EVERY generic call at `Typer:1814` before `checkWithDeferred`/`checkGenericTyped` look up candidates.
- `CallShapes.expandLetBoundLambdaArgs` (:100+) and the automap at `CallShapes:87-89`.

Under §1.1's rule ("the typer sees an unbound node and walls it") and §4's "Zero candidates is a
WALL", every one of these paths walls. `bindings.rekey(old,new)` is not enough for subtree rewrites:
`SourceSubst`/`AlphaRename`/`StaticFold.fold` have no old→new pairing at the call level (they map
children generically through `ValueSpecification.mapChildren`, `protocol/spec/ValueSpecification.java:220`).

**Change.** Either (a) make the rewriters binding-preserving by construction — a `withParameters`-style
copy carries the binding through a compiler-side `Bindings.Builder` visible to every rewriter (which
means the Builder is mutable for the whole compile, not "sealed at the end of resolve"), or (b) keep
the resolver's answer ON the node as a declaration-id list (the protocol node cannot hold `FunctionId`
because of the cycle, but it can hold `List<String>` of ids exactly as it holds `candidateFqns` today —
the cycle argument in §1.1 does not apply to a string list), or (c) resolve a synthesized/rewritten
node the way §1.3 proposes for `CallShapes` (`Bindings.bind(node,id)`) at all ~180 sites and pin the
count with a guard. The design must pick one and count the sites; "four rewriters" is off by ~45x.

### 2. BLOCKER — `Bindings.merged` "disjoint key sets, or IllegalState" is violated by the normalizer's own re-resolution of already-resolved nodes

**Claim** (§1.2): "`ModelNormalizer.resolveSynthesized` merges the per-body `ResolvedQuery.bindings()`
into the model's (`Bindings.merged`: key sets are disjoint because the synthesized bodies are fresh nodes)".

**Evidence.** The synthesized bodies EMBED parsed, already-resolved nodes: `normalizer/ModelNormalizer.java:320`
builds the constraint function with body `List.of(c.expression())` (the class's constraint expression,
resolved in phase D by `resolveClass`), `:340` `List.of(c.message())`, `:404` `body` for a service query;
the mapping realizers carry the mapping's own expressions likewise. `resolveSynthesized` (:149-187)
then runs `NameResolver.resolveQueryIn` over every statement, whose `AppliedFunction` arm returns the
SAME node object when nothing changed (`NameResolver:1747-1748`: `yield (fn.equals(af.function()) &&
params == af.parameters() && candidates.equals(af.candidateFqns())) ? af : new AppliedFunction(…)`)
— the javadoc at :146-148 says so: "Parsed text inside a body is already resolved and passes through
unchanged (resolution is idempotent)". Under Bindings that node is keyed once in the model's builder and
again in the query's builder → `merged` throws. Worse, the idempotency test §1.2 proposes
(`bindings.has(node)`) consults the QUERY's builder, which cannot see the model's builder, so the node
is re-resolved under the OWNER's import scope (`:169-170 parsed.elementImports().getOrDefault(fd.synthesizedFrom().ownerFqn(), none)`)
— possibly a different scope than the one that resolved it the first time.

**Change.** `resolveQueryIn` must take the prior `Bindings` as input (so `has(node)` is answered
against it) and `merged` must tolerate identical re-bindings, or the design must state that synthesized
bodies never embed resolved nodes (false today). Add the test: resolve a class with a constraint, then
normalize, and assert one binding per node.

### 3. BLOCKER — `findFunctionById` does not return "one overload per id" until 3d; 3a's consumption path IS the merge point 3d rewrites, so the pushes are not independently revertible

**Claim** (§1.1): "With ids the typer asks `ctx.findFunctionById(id)` (exists: `ModelContext:94`) — one
overload per id, no FQN fan-out". §0: "Each is small enough to hold in one head and to revert alone."

**Evidence.** `compiler/element/PureModelContext.java:365-380`:
```java
List<Function> defs = new ArrayList<>(model.findFunctionById(qualifiedId));
NativeFunctionDefinition n = Pure.nativeFunctionById(qualifiedId);
if (n != null) defs.add(n);
for (Function d : defs) for (TypedFunction tf : findFunction(d.qualifiedName())) if (d.equals(tf.definition())) out.add(tf);
```
A model twin and its catalog native share an id; both are looked up through `findFunction(fqn)` →
`FunctionCompiler.functionsAt(fqn)` (:58-83), which keeps both unless `isPlatformOwnedFunction` or the
PCT gate suppresses the model one — exactly the case the design says exists today ("the kernel's tie
tolerance `allSameShape` is what lets a catalog native and its bodied twin coexist", §0;
`InferenceKernel:1133`). So in 3a `bindings.of(af)` → `findFunctionById` yields 0, 1 or 2
`TypedFunction`s per id, and the result set is decided by the gates 3d deletes. 3a cannot be reverted
without 3d's semantics, and 3a's "CANDIDATES rows identical" proof passes only because the same gates
still run underneath.

**Change.** State it: 3a's candidate set is `bindings.of(af)` → `findFunctionById` → whatever
`functionsAt` admits; 3d changes what that returns. Drop "revert alone" for 3a/3d, or move the
one-declaration-per-id table (task #43) before 3a.

### 4. WRONG — `com.legend.compiler.Bindings` collides with the existing `com.legend.compiler.spec.Bindings`

**Evidence.** `compiler/spec/Bindings.java:22 public final class Bindings` is the kernel's type-variable
binding store, used as `Bindings b` throughout `InferenceKernel` (`:104 unify(Type, Type, Bindings b)`,
`:777`, `:1040`) and `Typer` (`:1961-1963`), with `compiler/spec/BindingsTest.java`. §3.2's pseudo-code
even writes `b = fresh Bindings` for the KERNEL's bindings in the same document that names the
resolver's value `Bindings`. **Change:** name the resolver's value something else (`CallBindings`,
`Referents`) before any code is written.

### 5. GAP — The design does not say whether the resolver still rewrites a single-match call's spelling to its FQN; both answers break something

**Evidence.** Today a single match REWRITES the node's function to the FQN and leaves candidates EMPTY
(`NameResolver:1744-1745`: `String fn = matches.size()==1 ? matches.get(0) : af.function(); candidates
= matches.size() > 1 ? matches : List.of()`). Readers key on that spelling: `ResolvedNames.referents:25-26`
(`if (af.function().contains("::")) return List.of(af.function())`), `StatementInline:202-203`,
`PkInference:97 ctx.findFunctionDefinition(af.function())`, `DeferredArgs.isOverCall:52 CoreFn.of(af.function())`,
the 33 `FAMILY_LOOKUP_BY_NAME` sites (`IdentityGuardrailTest:147`: "CoreFn.of(spelling) and
RowGetter.of(spelling) in the typer"). The plan says "the syntax node stops carrying spellings"; the
design's §1.2 says only that the arm "stops writing candidateFqns". If the rewrite stays, the node still
carries a resolved spelling (contradicting the plan); if it goes, every `contains("::")` reader and
every `CoreFn.of(af.function())` on a qualified-by-resolution name changes behaviour, and 3a's
"CANDIDATES rows identical" cannot hold. **Change:** decide and state; list the spelling readers.

### 6. GAP — Where the normalizer's and the boot layer's Bindings live is unspecified

**Evidence.** `Compiler.java:244 private record Layer(NormalizedModel model, ModelBuilder index)`;
`ModelNormalizer.normalize` (:110-138) returns a `NormalizedModel` (package `model`, which the design
says may never carry a compiler type); `PureModelContext.from(normalized, index, …)` (`:141-167`) is fed
by `normalizeWithSystem` (`Compiler:372`), not by `resolved.bindings()` directly. The boot layer is
resolved separately (`Compiler:302 normalizeLayer(NameResolver.resolve(boot), null)`) and reaches the
user context as `prior = boot().checked()` (`Compiler:238, :430`; `PureModelContext.CheckedLayer :102`);
boot-layer bodies are typed lazily through the USER context, so `ctx.bindings()` must contain the boot
bindings — `CheckedLayer` is not mentioned. **Change:** `Layer` and `CheckedLayer` gain the value;
`ModelNormalizer.normalize` returns (model, bindings); say so.

### 7. GAP — Two by-name candidate lookups have NO `AppliedFunction` node to key a binding on

**Evidence.** `Typer.functionCandidates(String)` (`:2509`) callers: `Typer:2538` (the bare path of
`candidatesOf`), **`Typer:2665`** (`functionCandidates(ref.fullPath())` — a `PackageableElementPtr`
function REFERENCE used as a value, resolved by the resolver's TYPE rule), and **`EvalChecker:86`**
(`t.functionCandidates(ref.fullPath())` after a failed `synth(new AppliedFunction(ref.fullPath(), rawArgs))`
at :84). Neither is a call node. The ten `functionCandidates(AppliedFunction)` callers all pass a node
(`CallShapes:66`, `Typer:595,1732,1751,1771,1836,1893`, `ReceiverOwnedFunctions:58`, `StaticFold:216`,
`LambdaBodies:94`) but several pass a node they just REBUILT (finding 1). **Change:** Bindings must also
key `PackageableElementPtr` (or the design keeps a by-id lookup for references and says so).

### 8. GAP — The ResolvedNames catalog fan-out is not covered by 3a's proof

**Evidence.** `ResolvedNames.referents:28-32` ALWAYS adds `BareNames.catalog(name)` at arity — for
nodes that carry candidates too, not only bare ones. 3a replaces it with `bindings.of(af)` ("NO catalog
fan-out", §1.3) — a behaviour change for the 33 sites (26 `names`, 7 `referents/declaredNatives`:
`PlanAllocations:455`, `StatementInline:285`, `ReceiverOwnedFunctions:55`, `ProjectChecker:257`,
`Typer:1093`, `StaticFold:439`, `MatchChecker:374`) that the CANDIDATES probe (`DecisionProbe.candidates`,
emitted only from `Typer.functionCandidates(af)` `:2530-2532`) never sees. **Change:** a probe row at
`ResolvedNames.referents` before 3a, or drop "CANDIDATES rows identical" as the sole gate.

### 9. NIT — Citation drift in §1 and §2 (all off by 1–9 lines; none changes meaning)

`NameResolver:362-381` → :362-378; `:372-374` (own-package tier) → :371-373; `:387-397` ✓;
`:641-705` → :635-711; `:678-684` → :681-687; `:696-702` → :697-701; `:694-700` (functionType
comment) → :693-696; `:1705-1750` → :1699-1752; `:1708-1709` → :1700-1703; `:1717-1737` → :1714-1743.
`FunctionCompiler.functionsAt :34-93` → :34-84; bare branch `:35-54` → :35-57. `ArchitectureTest:946`
→ :955 (`"com.legend.compiler.element.FunctionCompiler.SUPPRESSED_ONCE"`). "our tree at 16d2f8ba3" —
HEAD is 4c9c4364a (no relevant diff).

### 10. NIT — "five callers" of `PureModelContext.from`

`Compiler.java:238, :429` (two), `test/…/testing/Phases.java`, `test/…/compiler/OneIndexTest.java`, and
the two internal overloads. Fine as "five", but "five in Compiler" is two.

---

## B. The candidate rule (3b)

### 11. GAP — The probe's "4 resolver-added names" is 6, and the two unexplained ones expose a rule hole

**Evidence.** `receipts/…/step3/tiers/bare-tiers-pure-source.tsv` rows with site `resolver-added`:
`currentUserId` (ENGINE), `get` (ENGINE), `wtd` (ENGINE), `ytd` (ENGINE) — and **`flatten`**
(`meta::pure::functions::relation::variant::flatten`, tiers `ENGINE|FORM`, classified FORM) and
**`toString`** (`meta::pure::functions::relation::toString`, tiers `CORE`, classified CORE). The design
(§2.4, §5 table "resolver-added probe rows in Pure source 4 → 0") explains four.
- `flatten`: `relation::variant` is in neither the 29 nor the 32; the reference needs an import; ours
  reaches it through ENGINE or FORM. 3b deletes the prelude merge (§2.3(5)) but keeps the FORM tier "for
  the typer's bare path until 3d walls it" — so `flatten` resolves in 3b through the typer's bare path
  and walls in 3d unless the corpus file imports the package. Not in the record.
- `toString`: the CORE tier of the MERGE added `relation::toString` that the resolver's own core-group
  tier missed. The resolver's tier tests `knownFqns` ⊇ `PLATFORM_FQNS` (`:395`), whose function half is
  `Pure.userResolvableFunctionFqns()` (`NameResolver:339-346`), while `BareNames.catalogTiered` reads
  `Pure.nativeFunctionsAt` (`BareNames:99`) — the catalog is wider than the user-resolvable set. §2.3(1)
  says the index holds "every declared function (model definitions + catalog natives)" via
  `DeclarationTable` (`PureModelContext:584-591`, built from `Pure.all()`), which WIDENS the resolver's
  set to natives that are not user-resolvable today. **Change:** say which set the index takes and why;
  count the natives in `Pure.all()` minus `userResolvableFunctionFqns()`.

### 12. GAP — The tier classifier hides names the reference would not resolve

**Evidence.** `tools/untangle/bare_tiers.py:50-55`: ENGINE-ONLY iff some FQN's tier set is exactly
`{"ENGINE"}`; an FQN spelled by ENGINE **and** FORM (`flatten`) is bucketed FORM. But under the
reference's rule (imports ∪ core ∪ Root) neither ENGINE nor FORM exists; a FORM-served FQN outside the
core group is just as invisible to the reference as an ENGINE-served one. The honest count is "names
served only by ENGINE∪FORM in Pure source", which the receipt shows as ≥5.

### 13. GAP — The probe cannot see the engine tier's service through the 33 name-test sites

**Evidence.** `BARE-TIER` rows are written at two sites only: `NameResolver:1724-1729` (the prelude
merge, `resolver-added`) and `FunctionCompiler:44-46` (`merge`). `ResolvedNames.referents/declaredNatives/names`
(finding 8) call `BareNames.catalog` (`ResolvedNames:29`) with NO probe; a checker decision taken because
the engine tier spelled the FQN (`MatchChecker:374`, `ReceiverOwnedFunctions:55`, `ProjectChecker:257`,
`Typer.tdsSchemaDesugars/tdsGetterDesugars` — the profile's top `ResolvedNames` callers, 25/25 and 9/14
samples) leaves no row. The `captured == false` path (`NameResolver:1714-1716`: a name the resolver's
tiers did not find at all) IS visible as `merge` rows via `FunctionCompiler.functionsAt`, so that part
of §5 holds. **Change:** a `BARE-TIER … referents` row at `ResolvedNames:29` before 3b.

### 14. GAP — §2.3(1)/(3)'s "per-kind index" is not what 2.3(1) builds

§2.5 says "the index must be per KIND (functions vs elements) — which 2.3(1) already is"; §2.3(1) says
"The set `knownFqns` stays for TYPE names". `knownFqns` (`NameResolver:387-397`) holds classes, enums,
functions AND mangled ids in one kind-blind set — that is the `functionType` collision the resolver's
own comment describes (`:693-696`). Type position stays kind-blind under the design.

### 15. GAP — The dialect for a standalone QUERY has no carrier

§2.3(4) puts `Dialect` on `ParsedModel`. Queries are `ValueSpecification`s from `SpecParser.parse(query,
com.legend.parser.Dialect.LEGEND_LITE)` (`Compiler:515-516`, inside `lowerQuery`, the shared back half of
`plan`/`execute`), `NameResolver.resolveQuery(ValueSpecification)` (`:527`), `resolveQueryIn(vs, imports,
universe)` (`:544`), `Compiler.resolveQuery(statements, imports, ctx)` (`:763`, "every executor — the
harness's flip included — resolves through here"), and `ChannelB:262-264`. None takes a `ParsedModel`.
Note a parser `Dialect` already exists (`com.legend.parser.Dialect`, `ElementParser:359,594,…`
`dialect.refusesPlatformDialect()`); the design should extend it, not add a second. **Change:** the
`ResolvedQuery` entry points take the dialect; say which callers pass which.

### 16. NIT — "`Compiler.plan:515`" is `lowerQuery`

`:510-516` is `private static Lowered lowerQuery(…)`, the back half shared by `plan` and `execute`; marking
it LEGEND_LITE marks both — fine, but say so.

### 17. NIT — The differential numbers are not in the step-3 receipts

"OVERLOAD rows 450 / PACKAGE rows 11" come from `docs/GATES.md:5859`; the only differential file under
`receipts/plan-audit-2026-09-26/` is `differential-join-747ff1c11.tsv` (79 OVERLOAD, 7 PACKAGE, header
"overload disagreement 799 | package disagreement 28"). Cite the run that gave 450/11 or add its receipt.

---

## C. The kernel (3c)

### 18. WRONG — §3.1 and §8 contradict each other on collection-literal multiplicity, and §3.1's citation is to the wrong code

**Claim** (§3.1): "Collection literals type `[n]` (IVP:212) — today the checker's `TypedCollection` gets
`Bounded(n, n)` (`Typer:2043`), so this holds already". §8: "the literal's MULTIPLICITY is NOT the
reference's: ours is the SUM of the elements' bounds".

**Evidence.** `Typer:2043` is `typed[i] = typeFuncColSpec(raw.get(i), …)`. The literal is typed at
`Typer:2722-2762`: `:2744-2749` "multiplicity = the SUM of element bounds, not the element count
(audit-of-R1 …)", `:2750-2760` sums `b.lower()`/`b.upper()`, `:2761 new Multiplicity.Bounded(lo, hi)`.
§8 is right; §3.1 is wrong and its test list (C34) would be written against a false premise.

### 19. GAP — The "egress wall" §8 wants to re-point has no named reader; the wall that fired in the incident no longer reads what §8 assumes

**Evidence.** The comment's "§5 egress wall" is `docs/STAMP_DISCIPLINE_PROGRAM.md:1447-1451` ("reverse/sort
walling on a correct one-element result", 2026-08-21). The only "result shape" readers today are
`StatementExecutor.java:2565-2571` and `:2741-2747`:
`declaredInfo.multiplicity().requireBounded("result shape").isMany() && Type.isRelation(root.info().type())`
— `isMany()` (`Multiplicity.java:34-36`: upper null or > 1) is TRUE for both `[2]` and `[1..2]`, so THIS
reader does not distinguish the two typings; whatever fired in August (a lower-bound-sensitive check)
must be found before "two facts, two readers" can be designed. §8 names no reader.

### 20. GAP — `CallShapes`' automap trigger disagrees with KR C23, and CallShapes is kept as-is through 3a–3c

**Evidence.** `CallShapes:80 if (!recv.info().multiplicity().isMany()) return null;` with `isMany` =
`upper == null || upper > 1`: a `[0]` receiver and a multiplicity-PARAMETER receiver (not `Bounded`) do
NOT automap here; the reference automaps both (`!isToOne(m, strict=false)`, KR C23, `Multiplicity.java:78-83`).
§3.5 lists the C23 test but nothing in 3a–3c touches `CallShapes:80`.

### 21. WRONG — "`Typer:2803` is the qualified-property arm"

**Evidence.** `:2796-2804` is `liftedAccessor(RowGetter getter, …)` — the TDSRow/ResultSet row-accessor
lift (`getString(colName)` vs `getString(col:TDSColumn)`, javadoc :2792-2795). Qualified properties are
typed elsewhere: `Typer:569 applyGeneric(new AppliedFunction(owned.qualifiedName(), …))`, `:589-641`
("PARAMETERIZED qualified property: $p.synonymByType(X)…"), `:3020-3044`. Counting `lifted.size() > 1`
at :2803 (§7, §8) counts row accessors only; the unconditional-accept rule (KR B2) must be applied at
the `applyGeneric` arms, which today go through `resolveOverload`'s tie error (`InferenceKernel:1199-1209`).

### 22. GAP — `newTypeMatch` omits the extended-primitive check

KR A6 :302-305 `ExtendedPrimitiveType.testTypeVariableValuesCompatible(target, value)` (→ no match).
Our types have `Type.PrecisionDecimal` (`InferenceKernel:1360`); §3.1's table has no row for it.

### 23. NIT — KR contradictions in wording

- §3.2: "`expected` … binds the return type's variables from the context BEFORE the arguments when a
  parameter cannot" — the reference registers all arguments first (FEP:591), then makes the return type
  concrete from the ctx and REPROCESSES the arguments (FEP:503-514, KR A1). Same effect, inverted order.
- §3.2 step 4 "strict re-rank over ALL arity candidates" — FEP:210 re-ranks `mr.foundFunctions`, the
  LENIENT-matched list (FEM:63-76 drops null matches). Strict ⊂ lenient, so no outcome differs.
- §3.2 "Two paths bypass the loop" — KR B2 lists a third: a relation COLUMN (`_RelationType.findColumn`,
  FEP:306-310).

### 24. WRONG — §8's reading of `m3.pure:223,:228` and of our catalog

`:223` is the `genericType` of the property named `properties` of the class `Class` (`:221` name
'properties'), i.e. `Class<T>.properties : Property<T,Any|*>[*]` with the type-argument reference
carrying `contravariant:true`; `:228` is the same inside `Any.classifierGenericType`. It is not
"`AbstractProperty.genericType` is `Property<T contravariant, Any>`". The three `contravariant:true`
class declarations are `Property` (:1339), `PropertyRouteNode` (:3078) and `Column` (:3535) — not
"Property<U,V> and two siblings"; `ClassProjection` (:2772) spells `contravariant : false` explicitly.
"Our catalog spells those signatures `Property<Nil,Any|*>`": the only occurrence in `builtin/` is a
COMMENT (`Pure.java:216`, about `PropertyMapping.property`); no native signature spells `Property<Nil`.
`path.pure:17` is not in the pinned m3 archive (`find … -name path.pure` empty); it exists only in the
lagging `~/legend/legend-pure/…/platform_dsl_path/path.pure` (memory: read the pinned trees).

### 25. NIT — `isNative` readers miscounted and mis-cited

"`TypedFunction.isNative()` (13 readers incl. `Typer:1674`, `StatementInline:33`)": grep gives 13
`.isNative()` sites, of which **8** are `TypedFunction`'s (`ReceiverOwnedFunctions:65,85`,
`NumberKinds:55`, `UserCallInliner:239`, `Typer:1511,1601`, `InferenceKernel:1204`, `SignatureApart:71`)
and 5 are `ClassDefinition`/`TypedClass.isNative` (`FromProtocol:373`, `Pure:161`, `KnowledgeLayer:471`,
`NameResolver:737`, `ClassCompiler:101`). `Typer:1674` is `chosen.id()`; `StatementInline` has no
`isNative` (`:33` is javadoc). `FunctionCompiler:176` (not :186) derives it.

---

## D. TDS erasure (3.4)

### 26. BLOCKER — "The lowering already reads 'this value is a TDS' from the type" is false as cited; 45 `isRelation` readers see a nominal class after the switch

**Claim** (§3.4): "The lowering already reads 'this value is a TDS' from the type for the legacy api
(`PlatformTypes.RELATION_CARRIERS`, `:186-188`); what changes for it is that a legacy `project` result is
typed `TabularDataSet[1]`".

**Evidence.** `PlatformTypes.java:186-188 RELATION_CARRIERS = Set.of(RELATION, RELATION_ELEMENT_ACCESSOR,
RELATION_STORE_ACCESSOR, TDS_RELATION_ACCESSOR, TDS_RELATION_CLASS)` — `TDS_RELATION_CLASS` is
`meta::pure::metamodel::relation::TDS` (:182), NOT `TabularDataSet` (`:106 meta::pure::tds::TabularDataSet`,
tested separately by `isTdsType` :207-212). `Type.isRelation` (`Type.java:383-386`) is
`GenericType && RELATION_CARRIERS.contains(rawFqn) && arguments.size()==1` → **false** for
`TabularDataSet[1]`; `Type.relationValued` (`:446-450`) likewise. Readers of those two in the back half:
45 sites in `lowering/{RelationPredicates,Scalars,Lowerer,CollectionRelations}`, `resolver/{StoreResolver,
GenericTypeReflection,CorrelatedSubselects,Substitution,Anchors}`, `StatementExecutor` (`:2570, :2747`
"collectionDeclared"), plus the typer's own `rowCellRead` (`Typer:2814 Type.relationValued(grecv.info())`
— "a WRAPPED Relation<T> receiver is the rows collection … a bare struct receiver IS one row"),
`tdsSchemaDesugars`/`tdsGetterDesugars` (`:700`, `:343`) which need the SCHEMA the erased relation
carries. `isTdsShaped` (which does include `TabularDataSet`) has only 5 readers (`Anchors:394`,
`ResultEnvelopeSplice:585-598`, `TdsJsonChecker:57`). §3.4 inventories none of this; homework §1 says
"the lowering reads 'this value is a TDS' from the type as it already does for relations" without a
reader. The prior decision being reversed — `docs/TDS_ERASURE_DESIGN_2026_09_11.md` §4b "Model B: a TDS
IS a Relation<T> whose schema the checker knows … `TabularDataSet` stays nominal; R1 refines the result",
§7 "R1 is the KERNEL's output rule" — is not cited.

Also unaddressed: the PARAMETER half. `InferenceKernel:1362-1364, :1384-1386` let a `TabularDataSet`
formal accept a relation actual; under §3.1 it does not. Every legacy `tds::` call over a `#TDS{…}#`
literal (typed `TDS<T>`, a `RELATION_CARRIERS` member, `PlatformTypes:182`) or over a `tableToTDS` result
that is relation-typed today will lose its overload; the reference agrees only if those values are
`TabularDataSet` there too — not checked.

**Change.** Before 3c: list the 45 sites by what they decide when the value is `TabularDataSet[1]`;
decide where the schema of a legacy-projected TDS lives (a `RelationType` on the value, as R1 gives
today, or a side fact); probe the parameter half (`PICK` rows for every `tds::*` call over a relation
actual), not only the eight `size` rows.

---

## E. The merge point (3d)

### 27. GAP — "Zero candidates is a WALL" walls every minted node unless finding 1 is solved; the plan's tolerant loaders do not cover the typer

`resolve(parsed, walls)` (`NameResolver:201-205`) sinks RESOLVER walls; the zero-candidate case the design
describes is raised in the TYPER (`Typer:1838-1843` "unknown function … no function of this name"), which
throws (`compileAllBodies :1101` catches per body, `buildModule` :426-433 per element). A minted call that
walls poisons the whole body it sits in.

### 28. NIT — Pins: the design's 14 → 12 is right; the plan's 13 → 11 is stale

`IdentityGuardrailTest:148 Map.entry("FUNCTION_CATEGORY_CHECK", 14)`; `:140 NAME_COMPARE 208`,
`:146 CATALOG_LOOKUP_BY_NAME 10`, `:147 FAMILY_LOOKUP_BY_NAME 33` match §5.

---

## F. Homework §4 — the profile

### 29. WRONG (overstated) — "13–15% of the whole lane was that set build"

**Consistent part.** `profile-hotspot-pid-46696…txt`: `NameResolver` inclusive 256/1484; innermost
`resolveQuery(VS, ImportScope, Set)` 189; callees `Set.copyOf` 110, `AbstractCollection.addAll` 74 —
the causal claim (one method rebuilding platform ∪ model per statement) is what the receipts show.
`pid-46703`: 197/252, 107/80. The fix (`ModelNormalizer:161-172`, universe once; the 3-arg overload gone,
`NameResolver:527-546`) is the right fix.

**Overstated part.** 189/1484 = 12.7% and 197/1307 = 15.1% are shares of JFR EXECUTION SAMPLES
(Java threads on CPU, 10 ms period under `settings=profile`): 1,484 samples ≈ 15 s of an 85 s JVM;
189 samples ≈ 1.9 s ≈ **2% of that JVM's wall**; the child 197 ≈ 2 s of 38 s ≈ 5%. The timing receipts
agree with THAT: `jfr-run.log` (under JFR) `33s` / `42s`; `timing-after-resolver-fix.log` `32s` / `39s`
— **1 s / 3 s ≈ 3% / 7%**, and confounded (JFR overhead on the before run; load 2.41/5.47/8.61 at the
profile, 3.77/3.89/5.88 → 5.06 at the timing — not a quiet machine by the benchmarking rule). The
homework's own caveat ("a sample profile … is not a timing") is right; "13–15% of the whole lane" is
not. The design's §5 gate "before (16d2f8ba3): 32s / 39s" is the AFTER-fix number (16d2f8ba3 contains
the fix); the plan's "DuckDB ~32/37s" is a third number. **Change:** record the set build as "~13–15% of
sampled CPU, ≈2–5% of wall; measured 1 s/3 s on a loaded machine"; re-time quiet.

---

## G. Homework §5 — the tier probe

### 30. GAP — `!matches.contains(fqn)` is the right MARGINAL test for the prelude merge, but not for "what the tier adds"

**Evidence.** `NameResolver:1714-1729`: the merge (and its probe) runs only when `captured` — i.e. the
resolver's tiers found SOMETHING other than the bare spelling. When they found nothing
(`resolveNameMulti:711 return List.of(name)`), `captured` is false, no `resolver-added` row is written,
and the name reaches the typer bare → `FunctionCompiler:44-46` writes `merge` rows, which
`bare_tiers.py` DOES classify (a `{ENGINE}`-only FQN there would be ENGINE-ONLY; the receipt shows none
in Pure source). So the resolver→typer path is covered. The uncovered paths are finding 13 (the 33
name-test sites through `ResolvedNames`/`BareNames.catalog`, no probe) and finding 11/12 (`flatten`,
`toString`, the classifier's FORM bucket). "4 today" is a per-NAME count (`bare_tiers.py` dedups on
(name, fqn); `witness` is `<unattributed>` in every row), not a call count — the gate should say names.

---

## Summary of blockers (10 lines)

1. Identity-keyed `Bindings` vs ~180 post-resolution mint/rebuild sites (`SourceSubst:187`,
   `AlphaRename:39`, `StaticFold:203`, `StatementInline:121-171`, `Typer:1710-1727` applied at :1814,
   53 mints in `Typer`, ~77 in the checkers): every rebuilt node is unbound → walls. The design names four.
2. `Bindings.merged` disjointness is false: synthesized bodies embed resolved nodes
   (`ModelNormalizer:320,340,404`) and the resolver returns the same node (`NameResolver:1747-1748`).
3. `findFunctionById` (`PureModelContext:365-380`) returns twins via `functionsAt`'s gates until 3d;
   3a is not "one overload per id" and not revertible alone.
4. TDS: `Type.isRelation` is false for `TabularDataSet[1]`; `RELATION_CARRIERS` lacks it; 45 readers in
   lowering/resolver/executor + the typer's schema desugars; the prior Model-B design is reversed uncited.
5. Name clash: `com.legend.compiler.spec.Bindings` already exists.
6. §3.1 vs §8 contradict on literal multiplicity; `Typer:2043` is `typeFuncColSpec`, the sum is at :2744-2761.
7. No carrier for the normalizer's / boot layer's bindings (`Compiler.Layer:244`, `CheckedLayer:102`).
8. Single-match spelling rewrite (`NameResolver:1744-1745`) undecided; readers key on it.
9. Profile overstates: 13–15% of samples ≈ 2–5% of wall; receipts show 1 s/3 s on a loaded machine.
10. Probe: 6 resolver-added names not 4 (`flatten`, `toString`); 33 name-test sites unprobed.
