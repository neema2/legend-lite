# Phase G — SpecCompiler (type-check function bodies)

**Status:** Plan, pre-implementation. **Audited 2026-06-14** against the real engine module
(`legend-lite/engine`, 4 parallel source reads): LOC (`TypeChecker` 1,698 / `AbstractChecker`
1,311), the 37 `*Checker`s, `compiler/typed/*`, `ExpressionType`, and `compiled/CompiledFunction`
all verified present; the load-bearing algorithm + "removes" claims all **confirmed true**. Errors
the audit caught are corrected inline (see §11 audit log).
**Line numbers are approximate anchors** — engine method *names* are the durable reference; ranges
cite the method's neighborhood and drift ±10 lines (the audit re-pinned the ones that were wrong).

> **Companion docs:** `COMPILE_MODEL_AND_SIGNATURES.md` (Phase F — done; G's input
> is F's typed model + `TypedFunction` signatures), `CLEAN_SHEET_INVERSION.md` (why
> G is "just type-check a function body"), `CORE_PHASE_F_TYPED_ELEMENTS_V2.md`
> (the typed-element design; G outputs the `TypedSpec` half of it).

**Evidence basis (so each claim's confidence is legible):**
- ✅ **Verified against source this session** — engine algorithms/line-refs + the `*Checker` catalog +
  typed-HIR fields (4 audit agents over `legend-lite/engine`); the §7 "removes"; core's `Type.java`,
  `ModelContext`, `Pure.java`; §8 decimal rules + §9 G-α/G-η/G-ε (real-transpiler + GHC/OCaml research);
  §6 G.1 staging (legend-pure). These are *homework, not guesswork*.
- 🔧 **Design-intent (reasoned from the verified facts, not yet built)** — the `SpecCompiler` /
  `InferenceKernel` / `NativeRuleRegistry` structure (§3 note); the build order (§6); the per-variant
  core `TypedSpec` shapes (§5). Sound, but unbuilt.
- 🚧 **Explicitly unbuilt / forward** — `navigate` (§4.3, a planned primitive that does not exist in
  core yet); precision-agnostic subtyping *wiring* (§1.1 — the `basePrimitive()` hook exists, the
  unify/subtype use is G's job). Flagged inline.

---

## 0. What G is, in one line

**G = `SpecCompiler`: type-check a function body (`ValueSpecification`) → `TypedSpec`,
demand-driven, memoized by `functionKey`.** It is the `Spec` half of the
Element/Spec symmetry; F (`ElementCompiler`) is the `Element` half. F never opens a
body; **G is the only thing that opens a body.**

Because the inversion made *everything a function* (mapping transforms, derived
properties, constraints, service queries, user functions are all `FunctionDefinition`s
now), **G is one body checker for all of them** — not a type-checker-plus-mapping-pass.
This is the payoff: engine's `TypeChecker` is 1,698 LOC largely *because* it
entangles type-checking with mapping resolution; G sheds that (see §7).

---

## 1. The de-risking facts (so we don't fear one-way doors)

1. **The type system already exists and is complete for G.** `compiler/element/type/Type.java`
   already has `Primitive` (with `Family`), `PrecisionDecimal`, `ClassType`, `EnumType`, `TypeVar`,
   `GenericType(rawFqn, args)`, `FunctionType(params, result)`, `RelationType(List<Column>)`,
   `SchemaAlgebra(left, Op, right)`, and the `Param`/`Column` sub-records. Engine's whole generics
   machinery has a core counterpart. **No type-system one-way door** — it was built forward-looking in F.
   - **`PrecisionDecimal` (and integer width) stay — see §8.** Upstream omits them because it *executes
     in-memory*; LegendLite is a pure SQL transpiler, so every decimal becomes `DECIMAL(p,s)` and every
     integer `INT`/`BIGINT`/`NUMERIC(38,0)` — precision/width are part of the type we generate. The
     **hook** for precision-agnostic subtyping exists (`PrecisionDecimal.basePrimitive()→DECIMAL`,
     verified in `Type.java:165`), but *wiring it into unify/subtyping is G's job* — `isSubtype` today
     only walks class FQNs (verified `ModelContext:68`); primitive/precision subtyping is unbuilt. Cheap
     to add, but design-intent, not done.
   - **The relation two-level structure already matches engine (`Type.java:31, 224, 263`).**
     Engine has a dedicated `Type.Relation` *container* + a `Type.Tuple` *row/schema* type the
     `Relation<T>` type-param binds to. Core has the **same two levels**: the container is
     `GenericType(Pure.RELATION, [row])` and the row/schema type is `RelationType(List<Column>)`.
     So **engine's `Tuple` ≈ core's `RelationType`** — a rename (core's name is closer to Pure's
     own `RelationType`); columns are **not** flattened onto the container. Consequences:
     (a) **declared** relation types are two-level `GenericType(RELATION,[RelationType])` (verified:
     `DefaultModelContext:342` `Relation<…>`→`GenericType`, `:358` inline `(…)`→`RelationType`);
     (b) but per G-α (resolved by real-transpiler research), **computed relation values carry a bare
     `RelationType` row-struct** (Calcite-style), and the `GenericType(RELATION,…)` form is confined to
     Pure signatures. The **dedicated relational unify/resolve case (§3.2) is the bridge** between the
     two — keep it (it's also the fast path). See G-α.
2. **The input exists (verified).** `parser.spec.ValueSpecification` (the body AST) and F's typed model
   — `ModelContext.findClass→Optional<TypedClass>`, `findFunction→List<TypedFunction>` (a *list* —
   overloads, reinforcing G-γ), `findType→Optional<Type>` (verified `ModelContext.java:30/42/49`) —
   are both done. G consumes them.
3. **The native signatures exist — `builtin/Pure.java` (verified).** `Pure.ALL` is a flat
   `List<NativeFunctionDefinition>`, one constant **per overload** (e.g. 3 `asOfJoin`s), ported from
   legend-pure via engine's `BuiltinRegistry`. The signatures are **rich** — they carry type-vars,
   schema algebra in the return (`asOfJoin … : Relation<T+V>[1]`), function-typed params, and the agg
   fn1/fn2 split (`AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>`). `NativeFunctionDefinition`'s javadoc
   states the layering: *typing rules = the declared signature + per-function checkers.*
   - **One gap to fill:** `Pure` exposes only `all()` (flat). Engine's `BuiltinRegistry` also has the
     **name→`List<overload>` index + `resolve(name)`** the kernel needs. Add that index over `Pure.all()`
     (group by simple name); `InferenceKernel.resolveOverload` (§3.1) then scores within the candidates.
     *(Engine has two overload resolvers — `BuiltinRegistry.resolveSignature` (arity/context) and
     `AbstractChecker.resolveOverload` (scoring); core collapses to the one scoring resolver.)*
4. **The output is greenfield.** No `compiler/spec/` yet — the `TypedSpec` HIR (30 direct permits /
   ~52 concrete leaves, §5) is new. But it's *staged* (§5), and the engine catalog tells us every
   variant + field.

---

## 2. Native vs user: different type schemes, ONE checking framework

The distinction is real but it is about the callee's **type scheme**, not two algorithms:

- **Native functions are polymorphic schemes.** `map<T,V>(T[*],{T→V[*]}):V[*]` — type vars solved from
  the args, return *computed* by substitution.
- **User functions are monomorphic schemes** (zero free type vars — the degenerate case). Concrete
  types; the return *declared and validated*. (Core forbids `TypeVar` in user sigs — verified: engine's
  `classifyUserType` rejection, `TypeChecker:1202-1237`.)

Both type-check through **one bidirectional framework** — *corrected from an earlier "two irreconcilable
handlers" framing, which was wrong; verified against GHC, OCaml, Dunfield & Krishnaswami, Algorithm M.*
The clean encoding:

- **ONE kernel parameterized by an expected-type slot:** `typeBody(node, expected)` where
  `expected = Check(Type) | Infer(ref)`. Inference is just *checking against a fresh metavariable*
  (OCaml: `type_exp = type_expect (newvar())`). No separate infer/check traversals.
- **Native vs user is a property of the callee's TYPE SCHEME, not a separate code path:** a native is a
  *polymorphic* scheme (`map<T,V>…`); a user function is a *monomorphic* scheme = **zero free type
  vars** — the degenerate case. **One application rule covers both**: synthesize the callee scheme →
  *instantiate* it with fresh vars (a no-op for user fns) → *check* each arg against the instantiated
  param types → unification then **solves the native return automatically** and **validates the user's
  declared return automatically**.
- **The "two behaviors" are the two arms of ONE leaf helper** (GHC's `instSigma`): `Infer` arm =
  instantiate + bind the solved result (native); `Check` arm = subtype-check against the declared type
  (user). Not two kernels.
- **The only genuine mode-branch is lambda introduction:** a *checked* lambda unifies against the
  expected `A→B`; an *inferred* lambda invents a fresh arg metavariable. Keep that branch; collapse
  everything else into the `expected` slot.

(Engine's `compileLambdaBody` + `compileLambdaArg`/`compileUserCall` are the *un-refactored* version of
this — two call sites that happen to share a body routine. Core should adopt the `Expected`-slot form
directly: it's strictly cleaner and is what GHC/OCaml do.)

---

## 3. The framework to port (engine `AbstractChecker`, exact rules)

This ~1,300-LOC machinery is genuinely good and largely portable verbatim onto core's `Type`.

> **Structure note (don't copy engine's class split).** Engine split along the wrong axis:
> `AbstractChecker` fused *shared framework* with *base class for the 37 native checkers*, and
> `TypeChecker` fused *dispatch driver* + *user-function logic* + literal inference + property
> access + the mapping pass. Split along the **real seams** instead (composition, not inheritance):
> - **`SpecCompiler`** — the driver: `compileExpr(vs, bindings) → TypedSpec`, pure kind-dispatch, thin.
> - **`InferenceKernel`** — the good half of `AbstractChecker`, shared & stateless: `resolveOverload`,
>   `unify`/`unifyType`, `resolve`/`resolveOutput`, and the bidirectional `typeBody(node, expected)`
>   kernel (§2; engine's un-refactored `compileLambdaBody`). Held by composition, **not** a base class.
> - **`NativeRuleRegistry`** — a default `SignatureRule` (§4.1) + the ~22 bespoke `NativeRule`s (§4.2).
>   The native application rule (instantiate → check args → unify return) lives in the kernel, not per-rule.
> - **`checkUserCall`** — the monomorphic (zero-tyvar) case of the *same* kernel application rule (§2),
>   with `expected = Check(declaredReturn)`.
>
> This makes §2's one-bidirectional-framework the literal code shape, and replaces "37 checkers
> extending a framework base" with "a kernel + a registry of small native rules + a thin driver."

### 3.1 Overload resolution (engine `AbstractChecker:82-226`)
Exact algorithm — reproduce, don't approximate:
1. **Arity filter** (`:117-131`): keep defs with matching arity; fall back to variadic `[*]`.
2. **Single match → return** (`:132-134`).
3. **Structural match** on compiled arg types — invoked via `matchesStructurally` (`:138`, defined
   `:327-350`), which calls `structuralMatch` (`:359-443`) — handles platform pseudo-types
   (`RELATION`, `COL_SPEC`/`_ARRAY`, `FUNC_COL_SPEC`/`_ARRAY`, `AGG_COL_SPEC`/`_ARRAY`, `WINDOW`,
   `ROWS`, `RANGE`, `TRAVERSAL`, `FUNCTION`, `SORT_INFO`) and rejects lambdas/column instances in
   primitive/enum slots.
4. **Specificity scoring** (`:176-226`): per-param **type** score (exact=2, subtype=1, TypeVar=0)
   × 10 + **multiplicity** score (exact=5, graduated `[1]`=4 / `[0..1]`=3 / `[1..*]`=2 / `[*]`=1,
   Var=0). Highest total wins.
5. **Ambiguity → throw** (`:159-161`; tie *detection* sets an `ambiguous` flag in `scoreOverloads`
   `:189-194`) when top two tie. **Decision (G-β, closed): keep the throw,
   no tie-break.** The scoring already encodes subtyping specificity (exact=2 > subtype=1), so the
   more-derived overload wins the normal case by construction (`plus(Integer,Integer)` beats
   `plus(Number,Number)` on two `Integer` args). A *residual* tie means genuinely incomparable
   signatures — e.g. `f(Integer,Number)` vs `f(Number,Integer)` on `(Integer,Integer)`, both 2+1=3 —
   where an ambiguity error is the principled answer (as in Java/C#). Any tie-break would be arbitrary
   and could silently pick a wrong overload. Shared by native + user overload resolution; ties in
   practice arise from natives' dense overload sets (user functions rarely overload).

### 3.2 Unification (engine `AbstractChecker:488-666`)
Per `Type` variant, exact:
- **`TypeVar`** (`:575-597`): bind the actual unchanged; on re-bind conflict, reject *unless* one
  side is `Primitive.ANY` (the escape hatch).
- **`Primitive` / `PrecisionDecimal`**: subtype check that is **precision/width-agnostic** —
  `PrecisionDecimal.basePrimitive() → DECIMAL`, so `Decimal(p,s)` unifies/compares exactly as
  `DECIMAL` (and integer-width as `INTEGER`). Core centralizes this (no engine-style per-site
  normalization scar); precision/width ride on the type for codegen but are invisible to inference.
- **`EnumType`**: exact FQN match. **`ClassType`**: identity only.
- **Relation** (engine binds in `unifyLClass`, RELATION branch, `:698-711`: binds the type arg to
  `new Type.Tuple(rel.schema())`). **Keep a dedicated relational case in core too** — do *not* rely on
  generic-arg unification alone. Rationale: generic-arg unification reproduces the bind *only if every
  relation value is wrapped two-level* (`GenericType(RELATION,[RelationType])`); a **bare** `RelationType`
  value (inline relation literal, TDS literal, or a checker that returns `RelationType(cols)`) is a
  *different `Type` variant* from `GenericType(RELATION,[T])` and will **not** unify generically. The
  dedicated case ("actual is a relation, wrapped or bare → bind `T := RelationType(cols)`") bridges that
  robustly **and** is the fast path engine used (one bind vs rawFqn-compare + arg recursion + re-wrap, on
  the hottest path). When matching a `RelationType` against a concrete `RelationType`, match columns by
  name+type. *(Earlier-draft overclaim: "no trick needed" held only under an unstated wrapping invariant.)*
- **`GenericType`** (non-Relation): unify `rawFqn` then recurse pairwise into `arguments`.
- **`SchemaAlgebra`**: skip during unification (only appears in *return* types; resolved later).
- **Multiplicity-var binding**, and **multiplicity validation is SKIPPED for relation sources**
  (engine convention `:488-513`).

### 3.3 Resolution & output (engine `AbstractChecker:747-880`)
`resolve` substitutes vars; unwraps `GenericType(RELATION,[…])` back to a `RelationType`;
`resolveSchemaAlgebra` does `merge` (T+V) / `withoutColumns` (T-Z); unbound var → throw.
`resolveOutput` = `(resolve(returnType), resolveMult(returnMult))`.

### 3.4 The lambda kernel + helpers (engine `AbstractChecker`, methods scattered `:889-1310`)
The shared body kernel is engine's **`compileLambdaBody`** (`:977`) — the un-refactored equivalent of
our bidirectional `typeBody(node, expected)` (§2). Engine's **`compileLambdaArg`** (`:1049-1124`)
solves the lambda's param types from the surrounding args, then calls the kernel. Supporting: `extractFunctionType` (`:943`),
`bindLambdaParam` (`:889-911`, dispatch Relation-schema vs row vs ClassType), `validateLambdaReturn`
(`:985-1003`, where a `TypeVar` return is *inferred from the body* vs validated),
`desugarLambdaApplication` (`:1154-1179`, β-reduce literal lambda → `TypedLet` wrappers; used by
`eval`/`match`), `validateMult` (`:920-933`, only rejects `[*]→[1]`),
`findLowestCommonAncestor`/`resolveClassLCA` (`:1272-1310`), column-name extractors.

---

## 4. The checker catalog (the irreducible core)

Two tiers, dispatched by a **registry (name → checker), not a hardcoded switch**
(engine's `TypeChecker:669-734` switch is the thing to replace).

**Uniformity model (the consistency answer).** Every native — all ~34 — is uniform at three levels,
**regardless of implementation**: (1) a named `NativeRule` entry in the registry (`register("plus",…)`
— grep-able, discoverable); (2) a ported per-native `*CheckerTest` (`FilterRuleTest`, `FromRuleTest`,
…) — *isolation is a property of the test corpus, not of per-native code*; (3) identical dispatch
(`registry.get(name).check(call, ctx)`). They differ **only** in the rule's implementation. So we get
engine's per-native consistency/isolation **without** engine's per-native *classes* — which were the
wrong mechanism (~15 lines of ceremony each, and 12 hand-written near-duplicates *drift*; one shared
impl is strictly more consistent). Consistency = interface + registry + tests; DRY is an asset here.

*(2 of engine's 37 files are not native rules — `AbstractChecker` → the shared `InferenceKernel`,
`ScalarChecker` → the default `SignatureRule`; `FunctionChecker` folds into the driver.)*

**Two registries, composed (not duplicated).** `Pure.java` is the **signature** registry (data: name →
typed overloads, §1.3); the `NativeRule` registry is the **rule** layer above it (behavior: name → how
to type-check). The default `SignatureRule` *reads* a `Pure` signature and applies the kernel; bespoke
rules read it for overload candidates and compute output specially. This is engine's own split
(`BuiltinRegistry` + checkers), with engine's hardcoded switch (`TypeChecker:669-734`) replaced by the
registry. **Because `Pure` signatures carry schema algebra** (`T+V`, `AggColSpec<fn1,fn2,R>`), the
generic `resolve`/`resolveSchemaAlgebra` (§3.3) already computes much output.

**TWO axes, not one (verified — this corrects an earlier "zero-code" overclaim).** A native is
classified on *two* independent axes:
1. **Type rule:** *signature-driven* (kernel does it all — incl. relations, since the relational
   `unify`/`resolve`/`bindLambdaParam` cases are **in the kernel**, not per-native) vs *bespoke*
   (signature can't express the output — dynamic pivot cols, selective rename, β-reduction).
2. **Output node:** generic `TypedNativeCall` vs a **distinct `TypedX`** node that lowering must
   distinguish (a `filter`'s `WHERE` vs a `project`'s `SELECT`).

Engine's `FilterChecker`/`MapChecker` are **proof**: 47/66 LOC, *"fully signature-driven"*, identical
to `ScalarChecker` except they return `TypedFilter`/`TypedMap` instead of `TypedNativeCall`. So
relation-overloaded natives do **not** need a bespoke *type rule* — but they DO need a thin per-native
checker **for the node**. Truly "zero-code" = signature-driven **and** generic-node (scalar math,
comparisons). `filter`/`map` = signature-driven rule **+ ~2-line node constructor** = *thin*, not zero.
(See §5 for the node-vs-dispatch decision that determines thin-vs-zero.)

### 4.1 The default rule — `SignatureRule` (engine `ScalarChecker`)
The DEFAULT implementation, shared by every native whose type rule **is** its signature:
`resolveOverload → unify → resolveOutput → TypedNativeCall`. A trivial native is a one-line
registration reusing this instance — *still a named, tested, individually-addressable entry*, just no
bespoke code. The default rule supplies the **type logic**; the **output node** then splits by G-η:
- **Zero-code (default rule + generic `TypedNativeCall`):** scalar/collection natives — comparison ops,
  `let`, `get`, `write`, `from`, `toOne`/`toMany`/`to`/`toVariant`, `slice`/`limit`/`take`/`drop`, `zip`,
  `concatenate`, `flatten`, `sourceUrl`, the **object-graph overloads** of `filter`/`map`/`fold` (over
  `T[*]`), and plain scalars.
- **Thin (default type logic + a distinct-node constructor):** the **relation overloads** of `filter`/
  `map` (`TypedFilter`/`TypedMap`) and other relation ops — signature-driven *rule* (the kernel binds
  relation schema-vars + lambda return-vars, §3.2/§3.4) but a distinct `TypedX` node for lowering (G-η).

> **`arithmetic` is NOT on the default path** (`+`/`-`/`*`/`/`): per §8, decimal precision propagates
> via datatype rules (`PrecisionDecimal.times/plus/…`), so the *result type* isn't a pure signature
> substitution. Arithmetic is a **thin bespoke rule** (signature check + a datatype-precision call).
> This is the canonical "looks generic but isn't" — naming every native is what surfaces it.

### 4.2 Bespoke rules (~22, type rule ≠ signature)
Output schema/type depends on argument *values* (or on a datatype rule), not just the static
signature. Each reproduces an exact rule from the audited engine source (count is incidental — the
point is each is a named, tested registry entry like the default-path ones):

| Rule | Behavior (input → output), engine ref |
|---|---|
| `arithmetic` (`+` `-` `*` `/`) | signature check **+ decimal datatype precision** (`PrecisionDecimal.times/plus/…`, §8) — thin bespoke, not the default path. |
| `project` | `Relation<T>`/`C[*]` → `Relation<Z>` from col-specs (body type = col type); out `[1]`. `ProjectChecker:40-95` |
| `extend` | **core: scalar + window(`over`) ONLY** → `Relation<T ∪ new>`. Engine's traverse/association/embedded forms are **legacy** — clean-sheet does them via `navigate()` (§4.3) and nested `new`. `ExtendChecker` (port the scalar/window paths only) |
| `groupBy` | `Relation<T>` → `Relation<keys ∪ aggs>`; per-agg fn1/fn2 + aggregate-native resolution + extra-args + Number→actual refinement. `GroupByChecker:47-294` |
| `join` | `Relation<T>×Relation<V>` → `Relation<T∪V'>`; join-kind enum; condition `{T,V→Boolean}`; **prefix renames ALL right cols**. `JoinChecker:51-151` |
| `asOfJoin` | like join but **prefix renames only duplicates**; 2 condition lambdas. `AsOfJoinChecker` |
| `pivot` | `Relation<T>` → `Relation<groupCols ∪ dynamicPivotCols>`; dynamic col names unknown at compile time. `PivotChecker:41-93` |
| `select`/`rename`/`distinct`/`sort` | schema subset / rename / preserve+validate-cols. Thin custom (identity-or-subset schema + col-ref validation). respective checkers. *(`flatten` is signature-driven → §4.1.)* |
| `tableReference`/`tds` | → `Relation<schema>` where the schema comes from the **table/store definition or the literal** (not a signature). `TableReferenceChecker`/`TdsChecker` |
| `getAll` | `Class.all()` → `ClassType[*]`. **No mapping compilation** (that's Phase H). `GetAllChecker:35-56` |
| `new` | `^Class(props)` → `ClassType[1]`; validate props against class; **to-many scalar fixup** wraps in collection. `NewChecker:32-66` |
| `cast` | `Any[m]` → `T[m]`, T from `@Type`; `@Type` is **not** compiled as an expr. `CastChecker:38-75` |
| `if` | branches → `Type.commonSupertype`; result **always `[1]`** (engine returns `[1]` unconditionally, even for the no-else 2-arg form — `IfChecker:54`). *Flag: an else-less `if` arguably should be `[0..1]`; decide whether to match engine or fix.* `IfChecker:36-55` |
| `match` | **compile-time linear dispatch** → the matched branch body (no residual node). `MatchChecker:26-100` |
| `fold` | `T[*]` → `V[1]`; **4-strategy classification** (Concatenation/SameType/MapReduce/CollectionBuild) by AST pattern. `FoldChecker:45-209` |
| `eval` | 4 cases (ColSpec / element-ref / lambda / function-typed var). `EvalChecker:29-107` |
| `serialize`/`graphFetch` | graph-fetch tree build → `Variant[1]` / `Class[m]`. respective |

Shared helper to preserve: `compileTypedAggCall` (used by `groupBy`/`aggregate`/`pivot`,
engine `GroupByChecker:209-294`) — fn1/fn2 + aggregate resolution + extra-args + return refinement.

### 4.3 `navigate` — the clean-sheet navigation primitive (replaces legacy extend-navigation)

**This is where the inversion bites.** *(Design-intent — `navigate` is an unbuilt planned primitive;
verified it does **not** exist in core yet. The rest of §4.2 is ported from audited engine source; this
subsection is forward design from `COMPILE_MODEL_AND_SIGNATURES.md` Appendix A.)* Engine pulls related
data into a relation via `extend`'s traverse/association/embedded forms — legacy desugaring of
object-model navigation into table extends. Core does **not** carry those: navigation is the
first-class `navigate()` native
(`COMPILE_MODEL_AND_SIGNATURES.md` Appendix A — "rename from `associate`; dispatch through the
target's mapping; lower to a join"), and embedded mappings are nested `new` inside the `map` lambda.
So:
- **`extend` keeps only scalar + window** (the genuinely relational column-additions).
- **`navigate`** is its own custom checker + its own typed node (multiplicity per position; dispatches
  through the target's mapping; lowers to a join). Its *signature* is a Phase-F/`Pure.java` concern;
  its *semantics* (this checker) are Phase-G+.
- The legacy desugarer (B→E) emits `navigate()`, **not** extend-navigation — so by the time G opens a
  body, navigation is always `navigate()`. G never sees a traverse/association/embedded extend.

**Consequence for the typed HIR (§5):** core's `TypedExtend` drops `traversalSpecs` and the three
navigation extend-col variants (`TypedTraverseExtendCol`/`TypedAssociationExtendCol`/
`TypedEmbeddedExtendCol`) — those are engine-legacy. Core's extend-col seal is just
`TypedScalarExtendCol` + `TypedWindowExtendCol`. The navigation join-shape lives on `navigate`'s node.

---

## 5. The output: `TypedSpec` HIR (greenfield, staged)

`ExpressionType = (Type, Multiplicity)` on every node, exposed via `TypedSpec.info()` (engine
`ExpressionType` + `TypedSpec.java:61`). The seal is **two-level**: `TypedSpec` *directly* permits
**30** variants, one of which (`TypedNative`) is an intermediate sealed interface permitting **23**
applied-native records (`TypedFilter`/`TypedProject`/…) — so **~52 concrete leaves** total (the
earlier "~53" counted leaves, not direct permits). **Do not build all 52 at once.** Stage by what
each downstream needs, and make the *schema-in-`info()`* invariant load-bearing from day 1:
relational nodes carry their output schema inside a `Type.Relation` in their `info()` (engine
`ExpressionType.schema()` unwraps it), so lowering reads it rather than re-deriving — confirmed in
engine.

**One-way-door field layouts** lowering (Phase I) will hard-depend on — get these shapes right
even though lowering isn't built: `TypedProject`/`TypedExtend`/`TypedGroupBy` materialize the
output schema in `info()`; `TypedJoin.joinType` enum + `renames` map; `TypedWindowExtendCol.outerWrapper.holeName`;
`TypedAggCall` fn1/fn2/extraArgs/castType split; `TypedFold.strategy`; `TypedPropertyAccess` flat
`associationPath` (multi-hop flattened to one node, never nested); `TypedUserCall.callee` (resolved
function on the node, not a sidecar). These are structural (sealed dispatch + field presence), so the
compiler enforces them once defined.

> **Do NOT copy engine's legacy extend fields (§4.3).** Engine's `TypedExtend.traversalSpecs` and the
> `TypedTraverseExtendCol`/`TypedAssociationExtendCol`/`TypedEmbeddedExtendCol` variants are legacy
> object-model-navigation artifacts. Core's `TypedExtend` carries only `TypedScalarExtendCol` +
> `TypedWindowExtendCol`; navigation's join-shape lives on the `navigate` node instead.

**Node strategy (G-η, CLOSED — real-transpiler consensus).** Distinct `TypedX` node per **relational**
operator (`TypedFilter`/`TypedProject`/…), generic `TypedNativeCall` for **scalar** functions — the
unanimous Calcite/Catalyst/Substrait/DataFusion split (optimizer/lowering rules pattern-match on
relational node type; scalars are name-tagged). So relation ops get a thin per-native node constructor
(§4 axis 2); scalars are zero-code. The "generic node + dispatch-at-lowering" alternative is rejected:
real systems keep it for scalars, never for relational operators.

---

## 6. Build order (stages — each independently testable, green throughout)

The staging mirrors what we already shipped, so each stage *validates earlier work*:

- **G.1 — scalar/object core + the framework + user calls.** Literals (with precision/width per §8 —
  decimal scale from text, integer width from magnitude),
  `Variable`, `AppliedProperty` (scalar/class field access; flat `associationPath`), `new`, `if`,
  `let`, `match`, `collection` (element-LCA), the **object-graph collection ops** (`map`/`filter`/`fold`/
  `exists`/`sum` over a property's `T[*]` — the *object-graph* overloads, keyed off receiver type, **not**
  the `Relation<T>` overloads), the generic native path (§4.1 default rule), and the bidirectional
  application rule (§2) + `check(PureFunction)`. **Deliverable:** derived-property and constraint bodies
  type-check (verified against legend-pure: constraints are compiler-enforced `Boolean[1]` object-graph;
  derived props are object-graph in every real model — add a *defensive reject* if a relation-typed
  expression appears, since the grammar doesn't forbid it). **Service queries are NOT here** — they
  return either a relation (`…->project(…)` → G.2) or a graphFetch'd object graph
  (`…->graphFetch(…)->serialize(…)` → G.3). No relation algebra in G.1.
- **G.2 — the relation core.** `getAll`, `tableReference`, the `Relation<T>` overloads of
  `filter`/`map`, `project`, `extend` (scalar form first), `groupBy`, `join`,
  `select`/`distinct`/`sort`/`rename`/`slice`. Relation values carry a bare `RelationType` in `info()`;
  the dedicated relational unify case is the bridge (§3.2, G-α — settled). **Deliverable:**
  mapping-function bodies and **relational/TDS service-query bodies** (`…->project(…)`) type-check.
  *(GraphFetch services aren't relational — they finish at G.3; see below.)*
- **G.3 — the long tail.** `extend` window form, **`navigate`** (clean-sheet navigation, §4.3),
  `asOfJoin`, `pivot`, `fold` strategies, `eval`, `aggregate`, `serialize`/`graphFetch`, `from`,
  `cast`/type-conversions, `flatten`/`zip`/`concatenate`. **Deliverable:** **graphFetch service-query
  bodies** (`.all()->filter(…)->graphFetch(#{…}#)->serialize(…)` — object-graph + `graphFetch`/`serialize`,
  **no relation algebra**) type-check. So service bodies split: relational/TDS → G.2, graphFetch → G.3.
- **G.4 — wire-in.** Demand-driven invocation from F (a `TypedFunction`'s body compiles on first
  request), memoized by **`FunctionKey(String fqn, List<Type.Param> signature)`** (G-γ): a value
  record keyed on FQN + the param `(Type, Multiplicity)` list — overload-disambiguating, return type
  excluded, no `TypeVar`s (user/lifted sigs are concrete). Cache is plain `computeIfAbsent`
  (`FunctionKey → CompiledFunction`) — **no cycle guard**, because `checkUserCall` checks against the
  callee's *declared* signature from F and never opens a callee body to check a caller (§2); a body
  compiles only when its own `TypedSpec` is demanded.

**Test corpus = engine's TYPE-ONLY tests (corrected 2026-06-15).** An earlier draft said "port the
`*CheckerTest`" — **wrong**: 17 of 18 `*CheckerTest` extend `AbstractDatabaseTest` and are **DuckDB
integration tests** (`executeRelation(query)` → assert on *result rows*), so they exercise typecheck →
SQL → execute and **cannot run in a type-only phase**. The portable Phase-G corpus is the **type-only**
engine tests:
- **`compiler/CompileFunctionTest`** — function-based, asserts `parameters`/`returnType`/multiplicity/
  body-HIR-type/return-mismatch/memoization. **Ported verbatim → `core …/spec/CompileFunctionTest`
  (7/7 green); it immediately found a real gap (`NewInstance` unimplemented), now fixed.** This is the
  corpus for the function-compile path.
- **`compiler/TypeCheckerTest`** — table/relation-based (`#>{db.table}#->filter/sort/rename/limit`),
  asserts inferred `Schema` columns/types. **Needs `tableReference` + a store/table model in
  `ModelContext` (unbuilt)** before it can be ported; that's the trigger to build the relational-store
  side. Until then, the *behaviors* it pins (filter/sort preserve columns, rename changes a column)
  are matched on class-derived relations (e.g. `…->project(…)->filter(…)`).

**Discipline:** for each native, port its engine type-only assertions *first* (definition of done), then
implement. Use `*CheckerTest` query *strings* as additional type-check targets where a schema is
inferable, but do not expect to run them until lowering+execution exist.

---

## 7. What the inversion *removes* (do NOT carry forward)

Engine's `TypeChecker` god-class is mostly mapping entanglement that no longer exists:

- **No `compileTouchedMappings` worklist** (engine `:503-514` — an unbounded *fixed-point* worklist,
  not literally two passes). It existed to demand-compile mapping bodies discovered during
  type-checking. In our world mapping bodies are ordinary lifted functions, type-checked through the
  *same* `check(function)` path — no special pass.
- **No `mappingTarget` context coupling** (engine `:391-393`). Engine threads a hidden "which class
  is this mapping materializing" so `ExtendChecker` can validate association/embedded extends. In
  our world the lifted mapping function's **own declared return type IS the target class** — that
  context comes from the signature, not a sidecar/inverse-FQN-lookup.
- **No observation-map sidecar for mapping resolution** (`classPropertyAccesses` /
  `associationNavigations` / `mappingFunctions` → `CompiledDependencies`, engine `:61-83, 1501-1520`).
  Mapping realization is **Phase H**, driven by the canonical binding table (inversion §6, CSI-3),
  not by metadata G stamps. G's output is just the typed body.
  - *Keep the genuinely intrinsic bit:* if a later phase needs "which classes/properties did this
    body touch," that's a pure post-walk of the typed HIR, **not** mutable state threaded through G.

This is *why* G is far smaller than engine's 1,698 LOC: it's the framework + per-native rules + a
small dispatcher, with the mapping machinery deleted.

---

## 8. Precision & integer width — KEEP on the type (LegendLite is a SQL transpiler)

**Why we diverge from upstream here — deliberately.** legend-pure/legend-engine keep `Decimal`
precisionless and `Integer` width-less at the *language* level (confirmed: `visit(CDecimal) →
getGenericType("Decimal")`, `visit(CInteger) → Long.parseLong → "Integer"`; precision/scale live on
`meta::relational::metamodel::datatype::Decimal { precision; scale }`). They can, because they
**execute in-memory** — the JVM does `BigDecimal`/`long` math and precision only matters when a value
hits a column. **LegendLite has no runtime: it only transpiles to SQL.** Every decimal becomes a SQL
`DECIMAL(p,s)`, every integer an `INT`/`BIGINT`/`NUMERIC(38,0)`; SQL makes precision/scale mandatory
in DDL and propagates them through arithmetic. So for us, precision/width are **part of the type we
generate**, not optional store metadata. (legend-lite-engine's INT32/INT64/INT128 are exactly these
three SQL storage classes — a feature, not a bug.)

**This costs nothing in inference** because core already split codegen-info from type-checking:
`PrecisionDecimal.basePrimitive() → DECIMAL` (and integer-width → `INTEGER`) makes subtyping/overload
**precision-agnostic** — `Decimal(18,2) <: Number` regardless of `(18,2)`. Precision rides along for
lowering, invisible to unification (§3.2). The engine "normalize at 5 sites" scar came from *not*
centralizing this; core centralized it.

**Decisions (close G-ζ):**
1. **Keep `Type.PrecisionDecimal` and the integer-width primitives.** No removal.
2. **Source of precision/width:**
   - *base columns* — from the relational store column definitions (authoritative; same origin as
     upstream's store datatype);
   - *literals* — from textual form (`1.50d` → scale 2; bare `Decimal` → `DEFAULT_DECIMAL` (38,18));
     integer literal width from its value's magnitude (`INT`/`BIGINT`/`NUMERIC(38,0)`);
   - *grammar* — Float = optional `f`/`F` (bare `1.5` is Float); Decimal = required `d`/`D`.
3. **Precision propagation is a LegendLite *datatype rule*, owned by the compiler — NOT a SQL rule.**
   `Decimal(18,2) * Decimal(5,4) → Decimal(24,6)` because that's what LegendLite's `Decimal` datatype
   *means*, exactly as `Integer + Integer : Integer` is a type rule. It is backend-independent: the
   same expression yields the same `Decimal(p,s)` for every target. SQL is downstream and **conforms**
   to the type; it never defines it.
   - **Express the algebra on the datatype:** methods `PrecisionDecimal.times/plus/minus/dividedBy(other)`
     (and the max-precision bound 38, already core's `DEFAULT_DECIMAL` basis) in the type-system package,
     **no SQL imports**. The arithmetic native checkers call these — the rules are not smeared across
     checkers, and not in the backend.
   - **The formulas (verified — the universal MS-SQL→Hive→Spark→Calcite lineage; everyone implements
     this exact table).** For operands `(p1,s1)`,`(p2,s2)`:
     - `+`/`-`: prec `max(s1,s2)+max(p1-s1,p2-s2)+1`, scale `max(s1,s2)`
     - `*`: prec `p1+p2+1`, scale `s1+s2`
     - `/`: prec `p1-s1+s2+max(6, s1+p2+1)`, scale `max(6, s1+p2+1)`  *(note: no inner `+s2` — an
       earlier draft had `max(6,s1+p2+s2+1)`, which is wrong)*
     - `%`: prec `min(p1-s1,p2-s2)+max(s1,s2)`, scale `max(s1,s2)`
     All clamped to max precision 38 with a scale floor of 6 (Spark's `MINIMUM_ADJUSTED_SCALE`), optionally
     behind an `allowPrecisionLoss`-style flag (Spark default true). Implemented in Calcite
     `RelDataTypeSystem.deriveDecimal*Type`, Spark `arithmetic.scala`/`DecimalPrecisionTypeCoercion`,
     Substrait `functions_arithmetic_decimal.yaml`. We *adopt* it as our datatype semantics (upstream
     Legend is precisionless, so it gives us none to inherit).
4. **Phase I/J only *realizes* the type** — maps a canonical `Decimal(p,s)` to a concrete dialect column
   type, **inserts conformance `CAST`s** where a backend's native arithmetic would diverge (e.g.
   division scale), and raises a *realization* error if a dialect genuinely can't represent it. Dialect
   differences are conformance casts, **not** type redefinitions. Runtime overflow (values exceeding the
   declared precision) is the database's execution concern, outside our compile-time type system.
   - *This dissolves the earlier "G vs I split":* the rules **and** the max-precision bound are all
     compiler/datatype-domain; I's role is realization + conformance only.
5. **`UnitInstance`** — no unit subsystem; if a unit literal ever appears, fail loudly rather than
   silently erasing to `Float` (legend-lite-engine `:566-570` erased to `0.0`). No unit work now.

> **Asymmetry to remember:** decimal precision is *correctness-critical* (`DECIMAL(p,s)` mandatory +
> arithmetic propagation); integer width is *fidelity* (widening is always value-safe, so a missing
> width defaults safely to `BIGINT`/`NUMERIC`). Spend the care on decimals.

---

## 9. Decision journal (settle before/at G.1; one-way doors)

- **G-α (CLOSED — resolved by real-transpiler research: Calcite/Catalyst/Substrait/DataFusion).**
  Every production SQL IR carries a **concrete row struct on each node** (Calcite `RelRecordType`),
  computed bottom-up, and **none keeps a parametric `Relation<T>` in the IR** — that lives only at the
  API surface (`Dataset[T]`, `IQueryable<T>`) and is erased at lowering. So for core: **computed
  relation values carry a bare `RelationType` (the row struct) in `info()`** (= schema-in-info, §5);
  the `GenericType(RELATION,[…])` form is **confined to Pure signatures** (where unification binds it);
  the **dedicated relational unify/resolve case (§3.2) is the bridge** between signature-world generic
  and value-world struct — and the mechanism that makes the monomorphic user-return check compare
  correctly (it unifies, not naive-equals). No two-level wrapping of values needed.
- **G-β (closed).** Overload ambiguity: **keep the throw, no tie-break** (§3.1 step 5) — scoring
  already encodes subtyping precedence; residual ties are genuine incomparabilities.
- **G-γ (closed).** `functionKey = FunctionKey(String fqn, List<Type.Param> signature)` — value record,
  FQN + param `(Type,Mult)` list; overload-disambiguating; return type excluded; no `TypeVar`s
  (concrete sigs). Value not identity (serialization-stable, coalescing, testable; `Type` records give
  structural equality free). Memo is plain `computeIfAbsent`, no cycle guard — checking uses callees'
  *declared* signatures, never their bodies (§2, §6 G.4).
  - **Validated vs real compilers (GHC/ML/Rust, TAPL).** "Type a call from the callee's signature, never
    its body" is the *universal* rule (T-App / T-Fix seed the environment first), so recursion is **never
    rejected** — only ill-founded *type/value* cycles are (`type T = T`, `let rec x=x+1`). Engine's
    `detectCallGraphCycles` (rejecting cyclic *call graphs*) is **non-standard**, existing only to protect
    its recurse-into-bodies design from infinite-looping. Our flat signature-based `check` + memo-as-
    visited-set is the textbook model (≈ rustc's demand-driven queries). Proof:
    `SpecCompilerTest.compileReachable_mutualRecursionTerminatesWithNoCycleGuard`.
  - **REFINED (engine parity): the ephemeral memo should key by `TypedFunction` IDENTITY, not by the
    `FunctionKey` value.** Engine keys `compiledFunctions` by `IdentityHashMap<PureFunction>` and
    documents it as *"content-correct without a separate content hash"* — because a `PureFunction` is
    immutable and built once per overload, an *edit makes a new object* → cache **miss → recompile**,
    never a stale hit. Core's `findFunction` caches `TypedFunction`s the same way (one object per
    overload per context), so identity-keying is content-correct here too, and it **sidesteps the
    name-stale footgun** that `FunctionKey(fqn,sig)` (a *name* key) has on edits. My value-key rationale
    (serialization-stable / testable) optimizes for a *persistent* cache the ephemeral memo doesn't have.
  - **Cache LIFETIME is still the contract.** Bind the memo to the `ModelContext` (a field of it / a
    `CompilationSession`), exactly like `DefaultModelContext.functionCache`. Engine documents the same:
    on model change, *construct a new TypeChecker*. Edit → new context → new empty memo. Never let a
    body memo outlive its snapshot.
  - **Content-addressing is the deliberate upgrade for PERSISTENT cross-edit reuse** (incremental):
    key by `hash(canonical body AST) ⊕ hash(transitive dependency signatures)` — needs dependency
    tracking, premature now; adopt it *with* incremental compilation. Still invalidation-free.
  - **`FunctionKey` (value) keeps a narrower role:** a serialization-stable handle for *naming* an
    overload in tests/diagnostics or a persisted artifact — not the in-memory body memo's key.
- **G-δ (closed).** No mapping sidecar / two-pass / `mappingTarget` in G (§7) — mapping realization
  is Phase H off the binding table. This is the inversion's whole point; reintroducing any of it
  would re-create engine's god class.
- **G-ε (REVISED — earlier "closed" was wrong).** It is **bidirectional typing**: ONE kernel with an
  `Expected = Check|Infer` slot, not two handlers (research: GHC `instSigma`, OCaml `type_expect`,
  Dunfield & Krishnaswami, Algorithm M). Native = polymorphic scheme, user = monomorphic = zero-var
  scheme (degenerate); one application rule (instantiate → check args → unify return vs `expected`)
  covers both. Only lambda-introduction branches on mode. My prior "unifying the handlers is a dead
  end" was lore from attempts lacking the `Expected`-slot abstraction — **reversed** (§2).
- **G-ζ (closed — KEEP precision/width, §8).** Unlike upstream (which executes in-memory and keeps
  precision on the store), LegendLite is a pure SQL transpiler, so `PrecisionDecimal` and integer
  width stay **on the type** — they're the SQL column types we generate. Subtyping stays
  precision-agnostic via `basePrimitive()`, so inference is unaffected.
- **G-η (CLOSED — resolved by real-transpiler research).** Universal split: **distinct typed node per
  RELATIONAL operator** (Calcite `LogicalFilter`/…, Catalyst, Substrait, DataFusion all do — optimizer/
  lowering rules pattern-match on node type), **generic name-tagged node for SCALAR functions** (Substrait
  registry anchor, Calcite `RexCall`). So core: relation ops (`filter`/`project`/`join`/…) get distinct
  `TypedX` nodes = **thin** checkers (node constructor); scalar natives (arithmetic/comparison/string) use
  generic `TypedNativeCall` = **zero**. This cleanly resolves §4's thin-vs-zero by the relation/scalar line.
- **G-ζ.1 (closed — precision is a datatype rule, not a SQL rule, §8.3–4).** Decimal-arithmetic
  precision propagation is a **compiler-owned LegendLite datatype rule** (`PrecisionDecimal.times/plus/…`,
  no SQL imports), backend-independent. Phase I/J only *realizes* the canonical `Decimal(p,s)` into a
  dialect type and inserts conformance casts — it never redefines the type. No real "G vs I split": the
  rules + max-precision bound are all compiler-domain.

---

## 10. The shape of done

G is done when: every `*CheckerTest` assertion ported from engine passes against the new
`SpecCompiler`; derived-property / constraint / service-query / mapping-function / user-function
bodies all type-check through **one** `check(function)` path; the typed HIR carries schemas in
`info()` and the one-way-door field layouts (§5) so Phase I (lowering) can consume it without
re-deriving anything; and **none** of engine's mapping sidecar, two-pass, or `mappingTarget`
coupling exists. The driver should be a *small* dispatcher over a checker registry — if it starts
trending toward 1,698 LOC, something from §7 crept back in.

---

## 11. Audit log (2026-06-14, vs `legend-lite/engine`)

Four parallel source reads verified this doc against ground truth. **Confirmed true** (no change):
the overload scoring weights (§3.1) exactly (`scoreCandidate:223` = `typeScore*10 + multScore`; type
2/1/0, mult 5/4/3/2/1/0); TypeVar unify + ANY escape hatch (`:575-597`); mult-validation skipped for
`Type.Relation` sources (`:504-510`); `resolve`/`resolveSchemaAlgebra` merge·withoutColumns (`:747-819`);
engine's dedicated `Type.Relation`+`Type.Tuple` model; the native switch + `ScalarChecker` fallback
(`:669-734`); `classifyUserType` rejecting TypeVar/FunctionType/Relation (`:1202-1237`); **all of §7**
— `compileTouchedMappings` (`:503-514`), `mappingTarget` (`:391-393`), the observation sidecars →
`CompiledDependencies` (`:71/73/83`, `:1501-1520`); memoization by `IdentityHashMap<PureFunction>`;
`UnitInstance → 0.0 FLOAT` (`:566-571`); the §5 one-way-door fields (`TypedJoin.joinType`+`renames`,
`TypedExtend.traversalSpecs` + 5 sealed extend-cols *— present in engine but **core keeps only
scalar+window**, §4.3*, `TypedWindowExtendCol.OuterWrapper.holeName`,
`TypedAggCall.{fn1,fn2,extraArgs,castType}`, `TypedFold.strategy:FoldStrategy`,
`TypedPropertyAccess.{associationPath,physicalColumn}`, `TypedUserCall.callee:CompiledFunction`);
and the checker rules (Map/Project/Extend-5-forms/GroupBy+`compileTypedAggCall:209-294`/Join-rename-all/
AsOfJoin-rename-dups-2-lambdas/Pivot-dynamic/GetAll-no-mapping/New-to-many-fixup/Cast-@Type/Match-linear/
Fold-4-strategies/Eval-4-cases).

**Corrected by the audit:**
- `checkLambdaBody` → engine's real name is **`compileLambdaBody`** (`:977`); native handler
  `compileLambdaArg` (`:1049-1124`). Methods scattered `:889-1310`, not all in `:943-1179`. (§2, §3.4)
- "~53 variants" → **30 direct `TypedSpec` permits / ~52 concrete leaves** via intermediate
  `TypedNative` (23). (§5)
- `IfChecker` returns **`[1]` unconditionally**, not `[0..1]` when else-less (`:54`). Flagged. (§4.2)
- `FilterChecker` has a **second collection overload** `T[*]→T[*]`, not relation-only. (§4.2)
- `structuralMatch` is `:359-443` (invoked via `matchesStructurally:138/:327-350`), Relation binding
  is `unifyLClass:698-711`, ambiguity tie-detection `:189-194`. (§3.1–3.2)

**Latent engine bugs to fix, not copy** (noted, not blocking): `classifyInteger` (`:1669-1678`) has a
`Primitive.INT64` constant but **never returns it** — jumps INT32→INT128, skipping INT64 (a stale
3-way Javadoc over 2-way code; our §8 classifier should use all three properly). `classifyDecimal`
(`:1686-1695`) derives scale from text but **hardcodes precision=38** (our §8 datatype rules supersede).

---

## §12 — Engine-parity function checklist (the roadmap)

**ARCHITECTURE AS BUILT (2026-07-03).** The Phase-G checker is now the layered, generic design:
- **`SpecCompiler`** — driver only (memo, `compileReachable`, whole-body `check`).
- **`Typer`** — ONLY the forms (literals/variables/collections/property access/colspec/enum
  values) and the ONE generic application rule (overloads, deferred lambda/colspec slots,
  signature-driven outputs). `AppliedFunction` dispatch is two-tier: the parse name resolves to a
  sealed **`CoreFn`** exactly ONCE, and the exhaustive `switch` is pure one-line delegation.
  Strings die at that boundary — the HIR carries resolved `TypedFunction`s / per-construct nodes,
  never names.
- **`*Checker` classes** (one per construct, engine's checker layout minus the 1311-line
  god-base): each construct's SHAPE decisions, desugars, and HIR emission — `SortChecker`,
  `RenameChecker`, `ProjectChecker`, `GroupByChecker`, `IfChecker`, … — all ≤100 lines, reached
  only through the CoreFn switch. `Application` is the check/emit seam; `Args` extracts checked
  payloads for emission.
- **`InferenceKernel`** — the generic type checker. New since the rewrite: schema-algebra
  **constraints** in parameter position — `X⊆T` (actual colspec names select concrete columns
  from the bound `T`, accumulating by union on rebind) and `X=(?:K)` (single-column wildcard;
  the shared `K` carries a renamed column's type, a shadow `?K` mult binding carries its
  multiplicity). `~col` synthesizes as a first-class `ColSpec<(col:?)>[1]` value (the unknown
  `?` never leaks into an output schema).
- **CHECK/EMIT split.** Core relation ops are CHECKED generically (`resolveOverload` +
  constraints + `resolveOutput` of `T-Z+V`/`Z+R`/`T+V` returns) and only their HIR **emission**
  is per-construct (`TypedFilter`, `TypedSort`, `TypedRename`, `TypedSelect`, `TypedGroupBy`, …
  — lowering dispatches on the sealed node, never on a name). Bespoke CHECKING remains only where
  unification genuinely can't go: `if` (branch join/LUB), `let` (scoping), `new`
  (instance construction), `tableReference` (store lookup).
- **Deferred arguments.** Lambdas AND mapped colspecs (`~a:x|…`, `~a:x|…:y|…`) are *deferred
  slots*: overload selection uses the value args plus the deferred args' syntactic shape
  (Func- vs Agg-, scalar vs Array raw class), then each deferred slot types against its
  now-concrete parameter — `FuncColSpec<F,Z>` binds `Z` from the checked bodies;
  `AggColSpec<F1,F2,R>` types map/reduce per column in a **`Bindings.copy()`** (the array
  signature's `K`/`V` are shared only syntactically) and binds `R`.
- **Legacy = desugar.** Legacy TDS `project([lambdas],['names'])` → modern `~[name:lambda]`
  (engine `ProjectChecker`'s own rewrite); bare project `~prop` → identity lambda `x|$x.prop`;
  bare sort key `~col` → `asc(~col)`; array `rename(~[a,b],~[x,y])` → a chain of scalar renames
  (its signature has no `K`; positional pairing is beyond unification). Full behavior, one rule.

The authoritative "what's left" list. Source of truth = engine's **37 `compiler/checkers/*Checker.java`** files
plus the master dispatch in `TypeChecker.java` (48 dispatched names). Two checkers are infra, not features:
`AbstractChecker` → our **`InferenceKernel`** (✅), `FunctionChecker` → our **`SpecCompiler` driver dispatch** (✅).
`ScalarChecker` is the catch-all signature-driven path (✅ — every non-relational native rides it for free).

**Discipline (per [[port-engine-tests-as-spec]]):** for each row below, port the matching engine **type-only**
assertion *first* (it's the spec), then implement to green. `CompileFunctionTest` (function bodies) and
`TypeCheckerTest` (relation schemas, needs `tableReference`+store) are the portable corpora; the `*CheckerTest`
are DuckDB integration tests — their query *strings* are still a useful type-check corpus.

**Tests pin behavior; the checker SOURCE carries the overload lattice.** Before implementing any row, read its
engine `*Checker.java` in full — most are 200–400 LOC encoding *multiple overloads + legacy-TDS desugar paths*
that no single test exercises. The nuance is real, not incidental:
- `SortChecker` (305 LOC): 1 relation + **3 collection** overloads + legacy TDS `sort(Relation,String,SortDirection)`
  desugar + the `sortBy`(ASC)/`sortByReversed`(DESC) family. **`sort` is collection+relation, like `filter`.**
- `GroupByChecker` (408 LOC): **4 relation** overloads (ColSpec×ColSpecArray × AggColSpec×AggColSpecArray) +
  a class-source overload + legacy arity-4 TDS desugar with rowMapper sugar (corr/covar/maxBy/minBy/wavg).
Clean-room reimplementation loses this. **Read the checker, enumerate every overload + desugar, mirror them.**

**INVARIANT — a special form validates against its registered signature; it NEVER ignores it.** A bespoke
handler (`if`/`let`/`project`/`new`/`tableReference`/…) may *compute its result* specially — because its args are
non-value syntactic forms (bindings, thunks, colspecs, store refs) the generic value path can't synthesize — but
it MUST first **validate the call against the registered native signature** (via `kernel.resolveOverload`, exactly
as engine's checkers run `resolveOverload(...)` *then* cast). The signature is the contract; bypassing it is how a
registered signature silently goes vestigial and drifts out of sync with the parser/AST (the `tableReference`
`(String,String)`-vs-`(PackageableElementPtr,CString)` desync that prompted this rule). Do **not** hardcode a
parallel copy of the constraint in checker code (e.g. `typeIf` hardcoding `Primitive.BOOLEAN` instead of sourcing
it from `if`'s signature) — that copy drifts too. Pattern: `resolveOverload(findFunction(name), argTypes)` to
validate inputs, then compute the precise output bespoke (the signature's generic return — `Relation<Any>`, `T[*]`
— is intentionally looser than the real result).

**Corollary — source the OUTPUT from the resolved signature wherever it's concrete enough; only override what the
signature genuinely can't express.** The generic path already does this right: `resolveChosen` unifies args→params
into `Bindings`, then `resolveOutput` substitutes them into the declared return *type and multiplicity*
(`getAll<T>(Class<T>):T[*]` → `Person[*]` — full resolve+unify, not a still-generic return). Bespoke forms must not
throw that away: keep the `Resolution` from the validating `resolveOverload` call and read what it gives. E.g.
`tableReference` takes its result **multiplicity** from `sig.output().multiplicity()` (the signature's `[1]`), and
only the **type** is bespoke (the concrete `RelationType` schema, since `Relation<Any>` can't carry columns).
Hardcoding `Multiplicity.Bounded.ONE` would be the same drift-prone parallel copy the invariant forbids.

**Root cause was wrong signatures, not real exceptions.** Verifying our `Pure.java` against **real legend-pure /
legend-engine** (`legend-pure/.../platform/pure/essential/lang/flow/if.pure`, the relation `.pure` in legend-engine)
showed we'd copied engine-lite's *simplified* signatures, which dropped **multiplicity variables** (`<T|m>`, `T[m]`).
That flattening is what forced the "exceptions": with the real `if<T|m>(Boolean[1],{->T[m]},{->T[m]}):T[m]` and
`letFunction(String[1],T[m]):T[m]`, the standard `resolve→unify→resolveOutput` pipeline produces the correct
result multiplicity (the kernel already binds/substitutes mult vars — `cast`/`reverse`/`sort<T|m>` prove it). So:

- **`let`** ✅ — corrected signature to `T[m]`; `typeLet` now routes through `resolveOverload` like any call (no
  bespoke output). Multi-valued `let xs = [1,2,3]` preserves `[*]` (was impossible under the wrong `T[1]`).
- **`if`** ✅ — corrected signature to `if<T|m>…:T[m]`; result multiplicity is now the branches' common `m`, not a
  hardcoded `[1]` (fixes the §4.2 engine bug). Condition type sourced from the signature. (Branch *thunks* still
  can't run full `resolveOverload` — that part is irreducibly bespoke, as engine's `IfChecker` documents.)
- **`tableReference`** ✅ — validates inputs + sources output multiplicity from the signature.
- **`project`** — still owes the audit (bespoke legacy arity-3 path; source output mult from its signature).

**New discipline (INCREMENTAL, decided):** verify every signature against **real legend-pure/legend-engine** `.pure`
source, NOT legend-lite/engine (a lossy rewrite). Multiplicity variables are load-bearing — dropping them silently
breaks the pipeline and forces bespoke patches. We do this **per-function, when implementing that function's
checker** (not as one big upfront audit): grep the real `.pure`, mirror the signature exactly (type params, mult
params, every overload, return), then implement to green. Known drift to fix on contact: `map` (`[0..1]` overload),
`getAll` (milestoning overloads), legacy `project` (returns `TabularDataSet[1]`; we use `Relation<K>[1]` — a
deliberate TDS→Relation divergence, keep but document).

### 12.1 ValueSpecification forms — `synthesize` dispatch
| Form | Status | Engine test |
|---|---|---|
| literals `CInteger/CString/CBoolean/CFloat/CDecimal` | ✅ | CompileFunctionTest |
| `Variable` | ✅ | CompileFunctionTest |
| `AppliedProperty` (object prop **+** relation column) | ✅ | — |
| `AppliedFunction` (native + user call) | ✅ | CompileFunctionTest |
| `LambdaFunction` (single-expr, in calls) | ✅ | — |
| `PureCollection` | ✅ | — |
| `PackageableElementPtr` (class ref) | ✅ | — |
| `NewInstance` (`^Class(...)`) | ✅ | CompileFunctionTest |
| `Match` / `cast` annotation | ✅ | match branches + `@Type` (`TypedTypeRef` prototype values) |
| date/time/`%latest` literals | ✅ | precision-typed: Year/YearMonth→Date, day→StrictDate, time→DateTime; `%hh:mm`→StrictTime (added to catalog vs real legend-pure) |
| `EnumValue` (`JoinKind.INNER`) | ✅ | validated vs registered enum |
| `ColSpec` / `ColSpecArray` (bare + mapped + aggregate) | ✅ | first-class values / deferred slots |

### 12.2 Generic scalar natives — `ScalarChecker` path (no bespoke code)
Arithmetic (`+ - * / %`), comparison (`< <= > >= == !=`), boolean (`&& || not`), string ops
(`concat startsWith substring length …`), collection scalars (`size isEmpty first at forAll exists …`).
**Status ✅** — all ride `resolveOverload`; precision via §8 `PrecisionDecimal`. Sampled-verified, not exhaustively ported.

### 12.3 Bespoke checkers — the real TODO (34 features)
| Engine checker | Pure function(s) | Status | Corpus / note |
|---|---|---|---|
| `FilterChecker` | `filter` | ✅ | collection **+** relation; own `TypedFilter` node |
| `MapChecker` | `map` | ✅ collection · 🔜 relation row-map | own `TypedMap` node |
| `ProjectChecker` | `project` | ✅ legacy **+** `~[colSpec]` **+** bare `~prop` **+** class-source | legacy/bare desugar to the modern form |
| `GetAllChecker` | `getAll` | ✅ | `TypedGetAll` construct node (lowering's mapping anchor) + milestoning-date overloads |
| `NewChecker` | `new` | ✅ | CompileFunctionTest |
| `IfChecker` | `if` | ✅ | returns `[1]` (engine quirk, §4.2) |
| `LetChecker` | `letFunction` | ✅ | CompileFunctionTest |
| `TableReferenceChecker` | `tableReference` (`#>{db.tbl}#`) | ✅ | `ModelContext.findTable`→`RelationType[1]`; TypeCheckerTest (6 live) |
| `SortChecker` | `sort sortBy sortByReversed` (+`asc/desc/ascending/descending`) | ✅ relation (GENERIC) **+** `sortBy`/`sortByReversed` **+** legacy string-sort desugar (`sort(rel,'COL',SortDirection.DESC)`, `sort(rel,['A','B'])`) | collection `sort` rides generic |
| `RenameChecker` | `rename` | ✅ | FULLY generic vs `ColSpec<Z=(?:K)⊆T>`→`T-Z+V`; mult preserved via shadow-`?K`; array form desugars to scalar chain |
| `SlicingChecker` | `limit slice take drop first head` | ✅ relation (generic path) · collection | **no bespoke code** — correct `<T>(Relation<T>[1],Integer[1]):Relation<T>[1]` sig + G-α `resolve` (Relation→bare row) ride `resolveOverload`; TypeCheckerTest `limit` live |
| `SelectChecker` | `select` | ✅ | fully generic (`Relation<Z⊆T>`); no-arg + `~col` + `~[cols]`; `newTDSRelationAccessor` aliases here |
| `DistinctChecker` | `distinct removeDuplicates` | ✅ distinct · 🔜 removeDuplicates | whole-row + `~[cols]` narrowing, generic |
| `ExtendChecker` | `extend` | ✅ ALL forms: scalar, aggregate (whole-relation window), windowed `over(…)` incl. `rows(a,b)` frames | `over(~p)` = `_Window<(p:?)>` value; fragments MERGE at a shared var (partition ∪ sort keys) and validate against the source via the fragment-rebind rule |
| `GroupByChecker` | `groupBy` (+`agg`) | ✅ relation (4 overloads) **+** class-source **+** legacy arity-4 TDS desugar | per-column `K`/`V` via `Bindings.copy()` |
| `JoinChecker` | `join` | ✅ | `JoinKind` enum-value form + 2-param condition lambda; `T+V` union |
| `AggregateChecker` | `aggregate` | ✅ | `Relation<R>` |
| `AsOfJoinChecker` | `asOfJoin` | ✅ match + optional cond + prefix overload | `T+V`; **DELIBERATE DIVERGENCE**: prefix renames EVERY right column (same rule as `join`) — engine-lite prefixes only the overlapping ones, an inconsistency we judged a bug and do not carry |
| `PivotChecker` | `pivot` | ✅ | static group cols (source−pivot−value) on `info()`; pivoted cols data-dependent, concretized by `cast(@Relation<…>)`; aggs ride the node |
| `FromChecker` | `from` (runtime/mapping binding) | ✅ | execution-context refs type `Any[1]` (`isExecutionContextElement`); (mapping, runtime) slotted on `TypedFrom` |
| `ConcatenateChecker` | `concatenate union` | ✅ concatenate · 🔜 union | shared `T` enforces same schema |
| `FlattenChecker` | `flatten` | ✅ | source schema with column widened to Variant (engine-lite behavior; real-pure single-col sig is a documented divergence) |
| `FoldChecker` | `fold` | ✅ | generic typing + engine's 4-way `FoldStrategy` classification (Concatenation/SameType/MapReduce/CollectionBuild) |
| `EvalChecker` | `eval` | ✅ | 4 forms: ~col→property desugar, funcRef→call desugar, lambda β-reduction (body type), function-typed variable (declared return) |
| `MatchChecker` | `match` | ✅ | STATIC dispatch: first subtype-accepting branch; result = branch body's type (not `Any[*]`) |
| `CastChecker` | `cast` (`@Type`) | ✅ cast/to/toMany | `@Type` synthesizes as a PROTOTYPE VALUE (`target[1]`) — fully generic vs `cast<T|m>(Any[m],T[1]):T[m]`; incl. `@Relation<(…)>` shapes (the pivot idiom) |
| `TypeConversionChecker` | `to toOne toMany toVariant` | ✅ | to/toMany via `@Type` prototype values; toOne/toVariant plain generic |
| `GetChecker` | `get` | 🔜 | |
| `ZipChecker` | `zip` | 🔜 | |
| `TdsChecker` | `tds` (literal TDS grid) | ✅ | header-parsed schema (`col:Type` + first-row inference); unknown types fail loudly |
| `SerializeChecker` | `serialize` (+`json`) | ✅ serialize | tree validated recursively; `String[1]` from sig |
| `GraphFetchChecker` | `graphFetch` | ✅ | tree properties validated recursively vs owner classes; result = SOURCE type (sigs fixed from `String[1]` to `T[*]`) |
| `WriteChecker` | `write` | ✅ | `Integer[1]`; destination ref on the node |
| `SourceUrlChecker` | `sourceUrl data` | ✅ sourceUrl | fixed `(data:Variant)[1]` |
| `navigate` (clean-sheet) | `navigate` | ✅ | ALL THREE positions of MAPPING_CLEAN_SHEET.md §3: pre-map (`Relation<S+(alias:Target)>`, sub-row col [1] per §3.4), post-map (fills a DECLARED property, `C[*]` passthrough), inline (`T[*]` constructor slot). Parser: colspec expression bodies wrap as zero-param thunks, so `~firm: Firm.all()` is the literal surface syntax. Replaces engine `traverse`/`_Traversal` extend-nav (never ported) |

**Scorecard: 34 / 34 bespoke features done** (`tableReference filter map project(all forms) getAll new if let
sort(relation) rename select distinct extend(scalar) groupBy(relation+class) aggregate join asOfJoin concatenate
slicing(limit/take/drop/slice) asc/desc enumValue`), plus all §12.1 forms minus Match/cast, and the generic scalar
path (§12.2). **TypeCheckerTest: 42 green** (engine's 11 ported + 31 pinning the generic paths). The established
shape for the remaining rows: verify the signature vs real legend-pure, port/write the type-only test, then either
(a) it rides the generic check + an emission arm (most), or (b) it's a desugar (legacy TDS forms), or (c) it's a
genuinely new capability (window/`over`, `match`, `cast`, runtime refs for `from`). The checklist is COMPLETE. Residual niceties only:
`@Relation<(…)>` wildcard columns in cast; `legacyNavigate`'s unbound-`T` signature (the normalizer-emitted
legacy peer — superseded by `navigate` for new code) has never been Phase-G-typed and would need the same
treatment if mapping-lifted functions ever compile through G. `removeDuplicates` deliberately stays on the generic path (collection semantics,
not SQL DISTINCT; engine groups it into DistinctChecker only organizationally); `union` has no registered
signature on either side. The full engine checker roster is otherwise covered — every checker either rides
the generic kernel with a construct-node emission or carries its engine behavior bespoke where signatures
can't express it. The correctness batch
(2026-07-03) closed the SILENT-WRONG-TYPE class: tds/sourceUrl/flatten/pivot no longer collapse to
`Relation<Any>`, match/eval no longer collapse to `Any[*]`, cast/to/toMany and date/time literals type instead of
throwing.
