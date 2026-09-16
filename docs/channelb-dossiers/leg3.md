# Leg 3 — function-type common supertype

**Rows:** `testMatchWithFunctionsAsParam`, `testMatchWithFunctionsAsParamManyMatch`,
`testMatchWithExtraParamsAndFunctionsAsParam` (Channel B, Essential).
**Charted error:** `no common supertype for {Integer[1] -> Integer[1]} and {String[1] -> Integer[2]}`

> **HEADLINE.** The charter's §3 rule is **CORRECT** — and I have the engine's
> exact rule, verbatim. **But it is only the first of two walls.** Fixing
> `commonSupertype` alone moves all three rows from one compile error to a
> different one. The wall that actually keeps them red is
> `MatchChecker.branches` (§b.2), which the charter never mentions.

See `README.md` for the shared tenet quick-reference and provenance notes.

---

## a) REFERENCE SEMANTICS

*All citations under `/Users/neemsandv`. `LP` = `legend-pure/legend-pure-core/legend-pure-m3-core/src/main/`, `LE` = `legend-engine/`.*

### a.1 The three tests, verbatim

`LP/resources/platform/pure/essential/lang/flow/match.pure`

| Test | Lines | The lambda list |
|---|---|---|
| `testMatchWithFunctionsAsParam` | **78–84** (list at **:80**) | `[{a:Integer[1]\|1}, {a:String[1]\|[1,2]}, {a:Date[1]\|[4,5,6]}]` |
| `testMatchWithFunctionsAsParamManyMatch` | **86–99** (lists at **:88**, **:94**) | `[{a:Integer[1]\|1}, {a:String[1]\|[6,7,1,2]}, {a:String[*]\|$a}, {a:Date[1]\|[4,5,6]}]`; `lambdas2` is the same multiset reordered |
| `testMatchWithExtraParamsAndFunctionsAsParam` | **174–180** (list at **:176**) | `[{a:String[1],b:String[1]\|'1'+$b}, {a:Integer[1],b:String[1]\|$b}, {a:Date[1],b:String[1]\|'5'+$b}]` |

**The decisive structural fact.** Each has an *inline twin* in the same file
whose lambdas are written directly into the `match(...)` call instead of being
hoisted into a `let`:

- `testMatchWithFunctions` **:101–106** — byte-identical lambdas to `…AsParam` **:80**
- `testMatchWithFunctionsManyMatch` **:108–121** — byte-identical to `…AsParamManyMatch` **:88**/**:94**
- `testMatchWithExtraParam` **:161–172** — the 3-arg form, same shape

**All three twins PASS in our Channel B today; all three `let`-indirected
variants fail.** (Derivation: `CHANNELB_BURNDOWN_HANDOFF.md:29` gives Essential
297/327 = 30 non-passing; `:32-43` enumerates the 9-row ledger and `:45-58` the
21 winnable — 9 + 21 = 30, an exact partition, and none of the three twins
appears in either list.) **The `let` is the entire difference between green and
red.**

`match` is a native with two overloads, `match.pure:25` and `:27`:

```
match<T|m,n>(var:Any[*], functions:Function<{Nil[n]->T[m]}>[1..*]):T[m];
match<T,P|m,n,o>(var:Any[*], functions:Function<{Nil[n],P[o]->T[m]}>[1..*], with:P[o]):T[m];
```

### a.2 Where the reference computes the LUB (the collection-literal path)

`LP/java/.../m3/compiler/postprocessing/processor/valuespecification/InstanceValueProcessor.java`

- **:57–95** `process(InstanceValue …)`; **:94** calls `updateInstanceValue` for
  *every* InstanceValue, collection literals included.
- **:120–132** `updateInstanceValue`: single value → single path; otherwise
  **:130** `updateCompositeInstanceValue`.
- **:196–214** `updateCompositeInstanceValue`; **:206** is the LUB call:
  `GenericType.findBestCommonCovariantNonFunctionTypeGenericType(genericTypeSet, knownMostGeneralGenericTypeBound, sourceInfo, ps)`.
  **:211-212** — the collection's multiplicity is the element **count**.
- **:148–185** `getGenericType`; a lambda is not a `ValueSpecification`, so it
  falls to **:184** `Instance.extractGenericTypeFromInstance`, returning the
  instance's `classifierGenericType` (`LP/java/.../m3/navigation/Instance.java:84–88`)
  — for a lambda that is **`LambdaFunction<{…}>`**, not a bare FunctionType.

The engine's protocol compiler takes the identical route:
`LE/legend-engine-core/…/toPureGraph/ValueSpecificationBuilder.java:883–911`
(`visit(Collection)`), element type at **:899–901** =
`MostCommonType.mostCommon(transformed.collect(_genericType).distinct(), pureModel)`;
`…/handlers/inference/MostCommonType.java:23–26` delegates to
`GenericType.findBestCommonGenericType(sourceTypes, covariant=true, isFunction=false, ps)`.
(`PureList` at **:812–829** does the same at **:819**.) Note the engine
**dedupes** (`.distinct()`) before the LUB.

### a.3 THE RULE — read end to end

**`LP/java/.../m3/navigation/generictype/GenericType.java`**

- **:1420–1437** `findBestCommonCovariantNonFunctionTypeGenericType`: size 0 →
  `Nil` (**:1426**); size 1 → copy (**:1430**); otherwise **:1434** →
  `findBestCommonGenericType(…, covariant=true, isFunction=false, …)`.
- **:1363–1418** `findBestCommonGenericType`. Any null in the set → null
  (**:1365–1368**). `isFunction=false` → **:1414–1416** dispatch to
  `Support.getBestGenericTypeUsingCovariance` / `…UsingContravariance`.
- **:1370–1411** — the `isFunction=true` body, **the FunctionType rule**:
  - **:1373** `int size = Support.findFunctionParametersCount(genericTypeSet, ps)`.
  - **:1375–1379** — **arity mismatch (or any `Any` in the set) ⇒ `Any` when
    covariant, `Nil` when contravariant.** Verbatim:
    `return Type.wrapGenericType(covariant ? …M3Paths.Any : …M3Paths.Nil, ps);`
  - **:1382–1397** — per parameter slot `i`:
    - **:1394** parameter multiplicity = `Multiplicity.minSubsumingMultiplicity(...)`
    - **:1395** parameter type = `findBestCommonGenericType(parameterTypes, null, **covariant=false**, isFunction=false, …)` — **CONTRAVARIANT**
  - **:1399–1408** — the result slot:
    - **:1407** return type = `findBestCommonGenericType(returnTypes, **covariant=true**, …)` — **COVARIANT**
    - **:1408** return multiplicity = `Multiplicity.minSubsumingMultiplicity(...)`

So the charter's guess ("contravariant params / covariant returns") is
**CORRECT**, and the multiplicity rule it did not name is
`minSubsumingMultiplicity` on **both** parameter slots and the return slot.

**How a `LambdaFunction<FT>` set reaches that rule.**
`Support.getBestGenericTypeUsingCovariance` (**Support.java:303–489**):

- **:325–329** `Any` present ⇒ `Any` (absorbing top). **:340** `Nil` raw types dropped.
- **:380–384** exactly one distinct concrete ⇒ copy it.
- **:387–399** general case: C3 generalization linearizations
  (`GenericType.java:517–520` → `C3Linearization`), shortest chosen.
- **:406–419** — **the FunctionType special case**: if the head of the shortest
  linearization *is* a FunctionType, all sets must also be FunctionTypes, else
  **`Any`** (**:411–415**, comment: *"Any is the only thing in common between
  FunctionTypes and non-FunctionTypes"*); then **:418**
  `findBestCommonGenericType(functionTypes, true, true, …)`. For
  `LambdaFunction<FT>` the head is the *class* `LambdaFunction`, so this does
  **not** fire at the top level.
- **:420–453** finds `commonRawType` = first level present in every
  linearization (here `LambdaFunction`); if none, **`Any`** (**:449–453**).
- **:455–472** — for each type parameter of `commonRawType`, recurse: **:465**
  `boolean covariant = TypeParameter.isCovariant(typeParameter)`, **:467**
  covariance→`getBestGenericTypeUsingCovariance`. `LambdaFunction<T>` declares
  `T` with **no** `contravariant` flag (`LP/resources/platform/pure/grammar/m3.pure:2383`),
  and `TypeParameter.isCovariant` is `!getBooleanValue(contravariant, false)`
  (`LP/java/.../m3/navigation/typeparameter/TypeParameter.java:25–28`) ⇒ **`T` is
  covariant**. The recursion's elements are now bare FunctionTypes, so
  **Support.java:406** fires and the structural rule runs.
- **:473–486** multiplicity arguments of the common raw type also merge with
  `minSubsumingMultiplicity` (**:482**).

**The contravariant half — `Support.java:211–301`,
`getBestGenericTypeUsingContravariance`. It has NO FunctionType special case.**
In full:

1. **:214–218** empty ⇒ `Any`.
2. **:219–223** one ⇒ copy.
3. **:234–238** any `Nil` present ⇒ **`Nil`** (absorbing bottom).
4. **:240–250** partition into non-concrete (type parameters, keyed by name) and
   concrete-non-`Any`, deduped by `GenericTypeHashingStrategy` (**:523–574**,
   which hashes FunctionTypes structurally over params + return).
5. **:256–277** no concrete-non-`Any`: 0 type params ⇒ `Any`; 1 ⇒ that
   parameter; ≥2 ⇒ `Nil`.
6. **:286–290** exactly one concrete-non-`Any` ⇒ copy it.
7. **:293–297** if one raw type is a subtype of *all* the others ⇒ that one.
8. **:300** otherwise ⇒ **`Nil`**. *(This is why `match`'s formal parameter is
   spelled `Function<{Nil[n]->T[m]}>` — `Nil` is what the branch-parameter GLB
   actually produces.)*

**`Support.java:491–521` `findFunctionParametersCount`:** returns `-1` (⇒ the
`Any`/`Nil` bail at GenericType.java:1375–1379) if any raw type is `Any`
(**:502–505**) or if two members disagree on parameter count (**:513–516**);
`Nil` members are skipped (**:506**).

**The multiplicity LUB — `LP/java/.../m3/navigation/multiplicity/Multiplicity.java`:**

- **:645–665** binary `minSubsumingMultiplicity`: null in ⇒ null out
  (**:647–650**); **non-concrete (a multiplicity variable) on either side ⇒
  `[0..*]`** (**:651–654**); otherwise `lower = min`, `upper = max` with an
  unbounded upper absorbing (**:656–662**).
- **:676–717** n-ary form: size 0 throws (**:683**); size 1 returns it, or
  `[0..*]` if non-concrete (**:688**); ≥3 folds min/max with the same
  non-concrete ⇒ `[0..*]` rule (**:700–703**).

### a.4 Does `match` have its own typing rule? — NO in pure, YES-but-trivially in the engine

- **legend-pure:** `match` is a plain `native function` (`match.pure:25`, `:27`).
  There is no `match` special case in the m3 post-processor; the branch
  collection is typed by the ordinary `InstanceValueProcessor` rule above, and
  `T`/`m` are solved by ordinary generic unification against
  `Function<{Nil[n]->T[m]}>[1..*]`. **The common supertype is not optional here
  — it is the only thing that gives the second argument a type at all.**
- **legend-engine:** `Handlers.java:1726–1727` registers match's return as
  `res(funcReturnType(ps.get(1)), funcReturnMul(ps.get(1)))` — i.e. **exactly
  the return slot of the LUB'd function collection's FunctionType**
  (`Handlers.java:3186–3193` / `3196–3210`). No LUB of branch bodies, no
  per-branch refinement.

### a.5 The derived answers, per row — and the reference's own corroboration

Applying **a.3** to **a.1** (`Integer <: Number <: Any` from `m3.pure:1525–1542`,
`:1608`, `:1619`; `String <: Any`):

| Row | LUB of the `let` collection | match result |
|---|---|---|
| `…AsParam` (:80) | params: GLB(Integer,String,Date) = **Nil**, mult minSub([1],[1],[1]) = **[1]**; return: LUB(Integer,Integer,Integer) = **Integer**, mult minSub([1],[2],[3]) = **[1..3]** ⇒ `{Nil[1] -> Integer[1..3]}` | `Integer[1..3]` |
| `…AsParamManyMatch` (:88/:94) | params: GLB(Integer,String,String,Date) = **Nil**, mult minSub([1],[1],[*],[1]) = **[*]**; return: LUB(Integer,Integer,String,Integer) = **Any**, mult minSub([1],[4],[*],[3]) = **[*]** ⇒ `{Nil[*] -> Any[*]}` | `Any[*]` |
| `…ExtraParamsAndFunctionsAsParam` (:176) | param0: **Nil[1]**; param1: GLB(String,String,String) dedupes to one ⇒ **String[1]**; return **String[1]** ⇒ `{Nil[1], String[1] -> String[1]}` | `String[1]` |

**These are not my arithmetic — the reference's own DuckDB PCT manifest confirms
each independently.**
`pct/src/test/resources/oracle/EssentialFunctions_manifest.duckdb.json`:

- **:277–279** `…AsParamManyMatch` → `"Match does not support Non-Primitive
  return type..! Current return type : Any"`. That string is emitted by
  `LE/legend-engine-xts-relationalStore/…/core_relational/relational/pureToSQLQuery/pureToSQLQuery.pure:3139`,
  asserting the *match expression's* `genericType.rawType instanceOf
  PrimitiveType`. Combined with `Handlers.java:1727` (match's type **is** the
  LUB's return type), **the reference computed `Any` for exactly this
  collection.** ✔ matches row 2.
- **:280–282** `…AsParam` → `"Cast exception: Literal cannot be cast to
  SemiStructuredPropertyAccess"` — it *passed* the PrimitiveType gate at
  `:3139` and died later at `pureToSQLQuery.pure:3150`. ✔ consistent with
  `Integer[1..3]`, a PrimitiveType. (`testMatchWithFunctions` at manifest
  **:286–288** carries the *same* error — the inline twin and the `let` twin are
  indistinguishable to the reference.)
- **:271–273** `…ExtraParamsAndFunctionsAsParam` → `"No SQL translation exists
  for the PURE function 'match_Any_MANY__Function_$1_MANY$__P_o__T_m_'"` — the
  3-arg overload is never attempted, because `canProcessMatch`
  (`pureToSQLQuery.pure:3130`) gates on `$params->size() == 2` (**:3134**). ✔
  says nothing about the type, and contradicts nothing.

**Consequence:** all three rows are *engine-frontier* rows — the reference's own
relational backend cannot run them. Our platform is expected to beat it here
(we already beat it on the inline twins).

---

## b) OUR SEAM

*All under `/Users/neema/legend/legend-lite/core/src/main/java` unless stated.*

### b.1 Wall 1 — `InferenceKernel.commonSupertype` (today's error)

**`com/legend/compiler/spec/InferenceKernel.java:1238–1306`.** Every arm, in order:

| # | Lines | Condition | Result |
|---|---|---|---|
| 1 | 1239–1241 | `a.equals(b)` | `a` |
| 2 | 1245–1251 | both `GenericType`, same `rawFqn`, each exactly 1 arg, both args `RelationType` | `GenericType(raw, [unionRows])` |
| 3 | 1254–1256 | both bare `RelationType` | `RelationType(unionRows(...).columns())` |
| 4 | 1260–1268 | both `GenericType`, same `rawFqn`, equal arity | arg-wise recursive `commonSupertype` |
| 5 | 1277–1281 | `schemaView(a)` is 1-column **and** `schemaView(b) == null` | recurse on that column's type vs `b` |
| 6 | 1282–1284 | mirror of 5 | recurse |
| 7 | **1285–1293** | `nominalFqn(a) == null \|\| nominalFqn(b) == null` | **`throw new TypeInferenceException("no common supertype for " + a.typeName() + " and " + b.typeName())`** ← **the wall, :1291** |
| 8 | 1294–1296 | `ctx.isSubtype(fa, fb)` | `b` |
| 9 | 1297–1299 | `ctx.isSubtype(fb, fa)` | `a` |
| 10 | 1300–1304 | first shared ancestor, BFS nearest-first (`ancestorsOf`, :1345–1360) | that ancestor |
| 11 | 1305 | fallthrough | `Any` |

`nominalFqn` (**:1331–1342**) is non-null for exactly `Primitive`,
`PrecisionDecimal`, `ClassType`, `EnumType`, `GenericType`; **null for
`TypeVar`, `FunctionType`, `RelationType`, `SchemaAlgebra`** (`Type` is sealed
over 9 variants, `com/legend/compiler/element/type/Type.java:46–51`). So a
FunctionType pair reaches arm 7 unless it hits arm 1 — **the wall fires only on
*differing* function types**; `[{a:Integer[1]|1}, {a:Integer[1]|2}]` already works.

**The exact path today.**

1. ChannelB strips the adapter (`pct/src/test/java/org/finos/legend/lite/pct/channelb/ChannelB.java:271–316`,
   `eliminateAdapter`: `$f->eval(|expr)` → `expr`). The `let lambdas = [...]`
   statement is **not** touched: `lets` only records let-bound *zero-param
   lambdas* (**:275–285**). The result is wrapped as a zero-param lambda
   (**:231**) and driven through `Compiler.executeResolved` (**:237**).
2. `Compiler.java:663` → `SpecCompiler.typeQueryBody` (**SpecCompiler.java:192–210**):
   statements typed in order; `TypedLet` binds `let.value().info()` forward (**:200–202**).
3. Statement 0 is the `let` → `LetChecker.check` (**LetChecker.java:24–40**),
   whose **:36** `t.synth(params.get(1), env)` on the `PureCollection`.
4. `Typer.java:186` `case PureCollection coll -> collection(coll, env)`.
5. Each element is a fully-annotated bare lambda → `Typer.java:216–262`;
   **:257–261** mints `ExprType(Type.FunctionType(params, result), [1])` — a
   **bare** `Type.FunctionType`, no `LambdaFunction<…>` wrapper (this is where
   we diverge in *spelling* from the reference, §a.2).
6. **`Typer.java:2471–2473`**: `elements.stream().map(e -> e.info().type()).reduce(kernel::commonSupertype)`
   — a **pairwise left fold**, not the reference's n-ary set operation.
7. First reduction: `{Integer[1] -> Integer[1]}` ⊔ `{String[1] -> Integer[2]}` →
   arm 7 → throw. `Type.FunctionType.typeName()` (**Type.java:329–334**)
   produces exactly the reported string. `TypeInferenceException` ⊂
   `LegendCompileException` ⊂ `RuntimeException`
   (**com/legend/error/LegendCompileException.java:24**), so ChannelB buckets it
   `ERROR` (**ChannelB.java:244–248**).

**Per-row error texts differ** — the charter's table gives one string for all
three. Row 1 is the quoted one; row 2's first pair renders
`… and {String[1] -> Integer[4]}`; row 3's renders
`no common supertype for {String[1], String[1] -> String[1]} and {Integer[1], String[1] -> String[1]}`
(params joined with `", "`, **Type.java:331–333**).

### b.2 Wall 2 — `MatchChecker` cannot see branches behind a variable (NOT in the charter)

`Typer.java:1287` dispatches `case MATCH -> MatchChecker.check(this, af, env)`
unconditionally.

**`com/legend/compiler/spec/MatchChecker.java:299–308`:**

```java
private static List<LambdaFunction> branches(ValueSpecification vs) {
    if (vs instanceof LambdaFunction lf) { return List.of(lf); }
    if (vs instanceof PureCollection c && !c.values().isEmpty()
            && c.values().stream().allMatch(v -> v instanceof LambdaFunction)) { … }
    throw new TypeInferenceException("match expects a collection of branch lambdas");
}
```

The branch list is acquired **syntactically**. In all three failing rows the
second argument is `$lambdas` — a `com.legend.protocol.spec.Variable`.
`branches` is first evaluated as an argument at **MatchChecker.java:58**, so the
moment Wall 1 is removed, all three rows fail with **`match expects a collection
of branch lambdas`**.

Nothing upstream can save it:

- `Env` (**com/legend/compiler/spec/Env.java:18–58**) carries **only**
  `Map<String, ExprType>` — no source, no `TypedSpec`.
- `SpecCompiler.check:168–170` and `typeQueryBody:200–202` both discard
  everything but `.info()`.
- `SourceSubst.inlineLets` (**SourceSubst.java:41–54**) exists and is exactly
  the right β-substitution, but is only invoked at *lambda* scope:
  `Typer.java:1461`, `:1554`, `:2055`; `EvalChecker.java:82`;
  `IfChecker.java:136`; `StaticFold.java:479`. Never on a top-level query/function body.
- `UserCallInliner.inlineBody` (**UserCallInliner.java:100–131**) does β-reduce
  query lets — but it runs on **typed** HIR (G½), long after `MatchChecker`
  needed the branches. And **UserCallInliner.java:418–430** confirms the
  architecture: `case TypedMatch m` is treated as a β-redex whose branch was
  *already chosen by the checker*. Branch selection is a TYPE-phase obligation
  (Invariant 1, `AGENTS.md:150`).

**This is why the twins pass and these three fail, and why the LUB is a red
herring for the actual green.**

### b.3 The type model the design can name

- `Type` sealed, 9 variants — `com/legend/compiler/element/type/Type.java:46–51`.
- `record FunctionType(List<Param> params, Param result)` — **Type.java:322–335**
  (`List.copyOf` in the compact ctor ⇒ well-defined structural equality).
- `record Param(Type type, Multiplicity multiplicity)` — **Type.java:591–601**.
- `record ClassType(String fqn)` — **Type.java:255–264**;
  `PlatformTypes.NIL = "meta::pure::metamodel::type::Nil"`
  (**com/legend/compiler/element/type/PlatformTypes.java:49**), `ANY` at **:48**.
- `sealed interface Multiplicity permits Bounded, Var` —
  **com/legend/compiler/element/type/Multiplicity.java:19**;
  `record Bounded(int lower, @Nullable Integer upper)` **:156–196**.
- **`Multiplicity.union(a,b)` — Multiplicity.java:83–96** is already **exactly**
  `minSubsumingMultiplicity` for two `Bounded`s (`min` lower, `max` upper, `null`
  upper absorbing, **:84–88**). **Divergence:** for a `Var` it returns the var
  only if both sides are the *same* var (**:89–92**) and otherwise **throws
  `IllegalStateException`** (**:93–95**); the reference degrades to `[0..*]`
  (`LP/…/Multiplicity.java:651–654`, `:688`, `:700–703`).
- Contravariance machinery already exists in the kernel:
  `Bindings.enterContravariant/exitContravariant/contravariant`
  (**Bindings.java:32–42**), used by `unify`'s FunctionType arm
  (**InferenceKernel.java:188–250**, with the `Nil`-formal skip at **:201–203**
  and the swap at **:215–219**).
- `unwrapFunction` (**InferenceKernel.java:1320–1328**) normalizes
  `Function<FT>`/`LambdaFunction<FT>`/… (carrier set at **:1313–1318**) to a bare
  `FunctionType`. Applied in `unify` (**:68–73**) and `paramTypeScore`
  (**:1056–1058**) but **NOT in `commonSupertype`**.
- `ModelContext.isSubtype` — **com/legend/compiler/element/ModelContext.java:231–265**,
  with the explicit `Nil`-is-bottom arm at **:250–252**.
- Our `match` signatures are transcribed verbatim from the reference:
  **com/legend/builtin/Pure.java:1872** and **:1874**.

### b.4 All `commonSupertype` call sites (exhaustive, incl. tests)

| Site | Role |
|---|---|
| `Typer.java:2473` | collection-literal element type (pairwise `reduce`) — **the failing path** |
| `IfChecker.java:69` | `if` then/else branch join |
| `IfChecker.java:112` | `if(condList, last)` fold |
| `MatchChecker.java:169` | `TypedMatchRuntime` arm-body join |
| `InferenceKernel.java:502` | covariant type-var rebinding — guarded `ClassType × ClassType` only |
| `InferenceKernel.java:1265` | self-recursion, same-raw generic args |
| `InferenceKernel.java:1280`, `:1283` | self-recursion, scalar-subquery unwrap |
| `core/src/test/java/com/legend/compiler/spec/InferenceKernelTest.java:224`, **`:229`** | unit test — **`:229` pins that `commonSupertype(RelationType, FunctionType)` THROWS** |

---

## c) MINIMUM DESIGN — the decisions

**Tenet posture first.** Per `TENET_CHARTER.md:33–36` (C1.6), a common-supertype
rule over FunctionTypes is *model-space computation* — it runs with no database
attached and no data loaded — so "Java orchestrates / the database executes"
imposes **no constraint on this leg at all**. Nothing here touches lowering,
rendering, or execution. The binding constraints are `AGENTS.md:142–158`
(Invariant 1: the frontend is the single source of truth for types) and
`AGENTS.md:244–252` / `TENET_CHARTER.md:52–55` (C2.4: no fabricated value, no
default that loses a type). Both are satisfied **only** by transcribing the
engine's actual rule — which §a.3 gives verbatim — never by a plausible
type-theoretic substitute.

### D1 — Normalize function carriers at the top of `commonSupertype`

Call the existing `unwrapFunction` (**InferenceKernel.java:1320–1328**) on both
operands at entry, exactly as `unify` (**:68–73**) and `paramTypeScore`
(**:1056–1058**) already do. *Justification:* the reference reaches the
structural rule through the covariant type-argument recursion on
`LambdaFunction<T>` (`Support.java:463–469` + `:406–418`); we reach the same
place by unwrapping. Without this, `Function<FT1>` vs bare `FT2` takes arm 4,
then arm 7 and throws, while `FT1` vs `FT2` takes the new arm — two answers for
one question.

### D2 — Add exactly one arm: `FunctionType × FunctionType`, placed **before** the wall at :1285

Transcribe `GenericType.java:1370–1411`:

- **arity mismatch ⇒ `Any`** (`GenericType.java:1375–1379`, covariant branch).
  This is a *cited engine behaviour*, not a default — the engine computes `Any`
  here, so emitting `Any` is conformance and emitting a wall would be inventing
  a refusal the spec does not have. **No witness row exercises it** (all three
  have uniform arity), so a live probe should confirm nothing in the corpus
  starts widening (§e).
- **each parameter slot:** type = `commonSubtype(...)` (D3); multiplicity =
  `Multiplicity.union(...)` (`GenericType.java:1394–1395`).
- **result slot:** type = `commonSupertype(...)` recursively; multiplicity =
  `Multiplicity.union(...)` (`GenericType.java:1407–1408`).

Everything else stays loud. Do **not** import `Support.java:411–415`'s
"FunctionType vs non-FunctionType ⇒ `Any`". That would silently change
`RelationType × FunctionType` and break the deliberate audit pin at
`InferenceKernelTest.java:229`; the reference's leniency there is a known,
*deliberate* divergence of ours, and this leg has no witness for it. **The new
arm is reachable only where we throw today, so no currently-green path can
change behaviour.**

### D3 — Introduce the MEET (`commonSubtype`) — we do not have one

New private method in `InferenceKernel`, transcribing `Support.java:211–301`
restricted to what our lattice can express:

1. `a.equals(b)` ⇒ `a` (their dedupe-to-one, `Support.java:286–290`).
2. either side is `Nil` ⇒ **`Nil`** (`Support.java:234–238`).
3. either side is `Any` ⇒ the other (`Support.java:245–250`).
4. `nominalFqn` non-null on both and one `isSubtype` of the other ⇒ that one
   (`Support.java:293–297`).
5. otherwise ⇒ **`Nil`** (`Support.java:300`).

Do **not** add a FunctionType special case here — the reference's contravariance
path has none. This is what turns `GLB(Integer, String, Date)` into `Nil`, which
is precisely why `match`'s formal is spelled `Function<{Nil[n]->T[m]}>`
(`match.pure:25`) and why our kernel's `unify` already skips `Nil` formal params
(**InferenceKernel.java:201–203**).

### D4 — Decide the `Multiplicity.Var` policy, explicitly

Reference: non-concrete ⇒ `[0..*]` (`LP/…/Multiplicity.java:651–654`). Ours:
`IllegalStateException` unless the vars are identical (**Multiplicity.java:93–95**).
No witness row has a `Var`. **Recommendation: keep our loud behaviour and record
the divergence in the leg's commit message** — `Multiplicity.java:29–32` already
writes down the "post-G never sees a `Var`" doctrine, and degrading to `[*]`
would be exactly the "default that loses a type" C2.4 forbids. But this must be
a *stated decision*, because as written the throw is an `IllegalStateException`
(an ICE, not a `TypeInferenceException`) escaping through the new arm.

### D5 — Wall 2: give `MatchChecker` a legitimate route to the branches

**This is the decision that actually turns the rows green.** Two candidates,
both compile-time only:

- **(B) source channel — the minimum.** Add a second, additive map to `Env`
  carrying each `let`'s **source** `ValueSpecification`, populated at the four
  sites that already walk lets (`SpecCompiler.java:169`, `:201`;
  `Typer.java:246`, `:2108`). `MatchChecker.branches` gains one arm: a
  `Variable` resolves through it. Semantically exact and already-argued in-tree:
  `SourceSubst.java:24–28` documents that pure lets are non-recursive value
  bindings, so substituting a let's value for its variable *preserves semantics
  exactly*.
- **(D′) typed channel — the principled variant.** Carry the bound `TypedSpec`
  instead, and let `branches` accept a `TypedCollection` of `TypedLambda`s: a
  `TypedLambda`'s `info().type()` is a `FunctionType` whose `Param`s carry the
  declared branch type *and* multiplicity, which is everything `MatchChecker`
  reads (`:88–96`, `:288–296`), and whose already-typed body is what
  `TypedMatch` needs. This makes `match` dispatch on **function values**, which
  is what the reference actually does — and it is exactly the distinction the
  test *names* (`…AsParam` vs the inline twin).

**Recommendation: (B) for this leg** (smaller blast radius, no re-typing
semantics to re-derive), with (D′) recorded as the successor. **Reject** blanket
source-level let-inlining at query-body level: `TypedLet` statements are
load-bearing for `ConnectionLets.java:38`, `PlanAllocations.java:39`,
`StatementExecutor.java:118/183/228/603/705/967/978/3032`, `SeedableLets.java:35`
and `StoreResolver.java:168`.

### D6 — File placement (guard-file arithmetic)

`InferenceKernel.java` is **1449** lines and `commonSupertype` is **69**; +~50
for D2/D3 stays far under `FILE_LIMIT = 3500` and `METHOD_LIMIT = 250`
(`core/src/test/java/com/legend/CodeShapeGuardrailTest.java:34–35`).
`MatchChecker.java` is 309, `Env.java` 58, `Typer.java` 3194,
`SpecCompiler.java` 225. **`Lowerer.java` is exactly 3500** — this leg must not
touch it, and does not need to (`TypedMatch` is β-reduced away by
`UserCallInliner.java:418–430`; the runtime arm lowers via the separate
`lowering/MatchFold.java`, reached from `Lowerer.java:3126`).

---

## d) TRAPS

1. **`Lowerer.java` is at 3500 = `FILE_LIMIT` exactly**
   (`CodeShapeGuardrailTest.java:35`; `FILE_ALLOWLIST` at `:42–47` contains only
   `MappingNormalizer.java`). One added line fails the build. This leg needs no
   Lowerer change; if a probe suggests one, the seam-split precedes it
   (`CHANNELB_BURNDOWN_HANDOFF.md:21–22`).

2. **The charter's §3 is right about the rule and wrong about sufficiency.**
   `CHANNELB_BURNDOWN_HANDOFF.md:124–127` says the kernel "needs a FunctionType
   common-supertype rule". True — but §b.2 shows `MatchChecker.java:307` is the
   wall that actually keeps these rows red. A burn that lands only the LUB will
   see three rows move from one red message to another and may read that as a
   regression.

3. **The charter's single error string covers only row 1.** Rows 2 and 3 produce
   different texts (§b.1). Don't grep for the quoted string when verifying them.

4. **`Typer.java:2473` is a pairwise left fold; the reference is n-ary over a
   deduped set** (`ValueSpecificationBuilder.java:901` `.distinct()`;
   `Support.java:320`/`:229` hashing-strategy sets). For these three witnesses
   fold == n-ary (each checked: contravariant `Nil` and covariant `Any` are both
   absorbing, and `minSubsuming` is associative). It is **not** true in general
   over our multiple-inheritance class lattice, where `ancestorsOf` BFS
   (**InferenceKernel.java:1345–1360**) picks the *first* shared ancestor. Do not
   "fix" the fold in this leg; note it.

5. **Existing non-commutativity next door.**
   `commonSupertype(PrecisionDecimal, PrecisionDecimal)` returns whichever
   operand came second — audit **D42**, `docs/type-audit-2026-08/MASTER.md:122`,
   root-sited at `InferenceKernel.java:1331–1341` + `:1294–1298`, verified by
   falsifier V12. The new arm recurses into arms 8–9 for return types, so a
   mixed-precision decimal return would inherit D42. Not exercised by these rows;
   do not silently "improve" it here.

6. **Two disagreeing LUBs exist** — audit **D65**, `MASTER.md:174`:
   `InferenceKernel.commonSupertype:1238` vs `valueLub:554`. `valueLub`
   (**:554–559**) is reached from **:524** for value-kind type-var conflicts and
   returns `Number`/`Any` only. It will **not** learn the FunctionType rule. If a
   burn later routes function values through a type-var rebind, the two answers
   diverge again.

7. **`InferenceKernelTest.java:229` is a live pin** asserting
   `commonSupertype(RelationType, FunctionType)` throws, with the comment
   *"Non-nominal MISMATCH (function vs relation): LOUD, never silent Any"*
   (**:226**). The engine returns `Any` there (`Support.java:411–415`). Adopting
   the engine's rule wholesale would need this pin moved with a dated
   justification (`AGENTS.md:10–15`) — **don't**; scope the arm to
   `FunctionType × FunctionType`.

8. **A neighbouring red row rides the same throw.**
   `docs/LEG1_INNERJOIN_FAMILY.md:35–38` records
   `no common supertype for (address:String[0..1]) and String` — a bare
   `RelationType` meeting a primitive. Any change to arm 7's *tail* affects it.
   The `FunctionType × FunctionType` arm does not.

9. **`Multiplicity.union` throws `IllegalStateException`, not
   `TypeInferenceException`** (**Multiplicity.java:93–95**). Reached from the new
   arm on a `Var`, that surfaces as an ICE and buckets as `ERROR` with the wrong
   phase. See D4.

10. **Record equality / keying.** `Type.FunctionType` (**Type.java:322–327**) and
    `Multiplicity.Bounded` are records with defensive copies, so arm 1's
    `a.equals(b)` is sound. But `typeName()` is **not injective** and is used for
    comparison/keying at 4+ sites (audit D65, `MASTER.md:174`, citing
    `Type.java:116–120, 261–274, 307–314, 545–550`); `ClassType.typeName()`
    returns the raw FQN (**Type.java:260–263**), so a `Nil` parameter renders as
    `meta::pure::metamodel::type::Nil[1]` in messages — noisy but honest.

11. **Inliner hazards — checked, and believed clear, but they are the probe
    list.** After D5 the `let lambdas` binding is dead:
    `UserCallInliner.inlineBody` (**:100–131**) β-reduces query lets and *drops
    the binders*; `SeedableLets.withSeedableLetPrefix` (**SeedableLets.java:31–52**)
    does a **trial lowering** and silently skips any let that cannot lower, so a
    collection-of-lambdas binding will not reach the Lowerer.
    `reserveFreshNames` (**UserCallInliner.java:134–147**) only bumps
    `_i`-prefixed names, so branch parameter `a` is untouched. `queryLets`
    (**:126**) will hold the collection and is read by
    `StoreResolver.withLetBindings`; inert unless something reads `$lambdas`.
    **All of this is static reading — probe it (§e).**

12. **Pins move in the right direction — verify, don't assume.** Channel A does
    not list these three in its expectedFailures
    (`pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_EssentialFunctions_PCT.java`
    has exactly one `match::` entry, `:121`, for `testMatchWithMixedReturnType`),
    so today they count `WIRE-BUG` and then `frontier` (they *are* in the DuckDB
    manifest). Passing them increments `agreePass` and decrements `wireBug` —
    both pins (`ChannelBEssentialTest.java:164`, `:165`) are directionally safe,
    as is `trueWireBug == 0` (**:237**). The `pass >= 305` floor (**:78**) only grows.

13. **Corpus-wide widening risk from D2 is structurally bounded.** The new arm
    fires **only where `commonSupertype` throws today** (arm 7 is unreachable for
    a FunctionType pair that would otherwise have been handled — arms 2–6 all
    require `RelationType`/`GenericType`, and arm 1 is equality). Same for the
    recursion at **:1265**. **No currently-green expression can start widening.**
    The one genuinely new value the arm can produce out of thin air is `Any` from
    the arity-mismatch branch — probe it.

---

## e) CONFIDENCE + LIVE PROBES

| Claim | Level |
|---|---|
| The engine's FunctionType LUB rule (contravariant params + `minSubsumingMultiplicity`, covariant return + `minSubsumingMultiplicity`, arity-mismatch ⇒ `Any`) | **Very high** — read end to end at `GenericType.java:1363–1418`, `Support.java:211–521`, `Multiplicity.java:645–717` |
| The multiplicity LUB is `min`-lower / `max`-upper with unbounded absorbing, and `[0..*]` for non-concrete | **Very high** — `LP/…/Multiplicity.java:645–717` |
| `match` has no special typing rule in pure; its result is the LUB'd collection's FunctionType return slot | **Very high** — `match.pure:25/27`; `Handlers.java:1726–1727` + `:3186–3193` |
| The three derived LUBs in §a.5 | **High** — derived from source *and* independently corroborated by the reference's own manifest errors (manifest `:271–282`, `pureToSQLQuery.pure:3134/3139/3150`) |
| Wall 1 is `InferenceKernel.java:1291` reached via `Typer.java:2473` | **Very high** — the reported message is byte-reproducible from `Type.java:329–334` and the fold's first pair |
| Wall 2 (`MatchChecker.java:307`) blocks all three rows after Wall 1 is fixed | **High** — read from source; the twin-passes/`let`-fails partition has no other explanation. **Not executed. The single claim most worth probing first.** |
| Nothing downstream (inliner / SeedableLets / Lowerer) walls on a dead lambda-collection let | **Medium** — reasoned from `UserCallInliner.java:100–131`, `SeedableLets.java:31–52`; **UNVERIFIED by execution** |
| The `Multiplicity.Var` divergence never bites the corpus | **UNVERIFIED** — no census exists for `Var`s inside collection-literal lambda types |

### Probes (none were run — read-only)

1. **Cheapest first, before writing any code.** Temporarily make
   `commonSupertype`'s FunctionType pair return `Any` (a throwaway, *not* the
   design) and run
   `cd pct && mvn -o test -Dtest=ChannelBEssentialTest -Dchb.only=MatchWithFunctionsAsParam`
   (after `mvn -o -pl core install -DskipTests` — the stale-jar trap,
   `CHANNELB_BURNDOWN_HANDOFF.md:161`). **Expected: the error changes to
   `match expects a collection of branch lambdas`.** That single observation
   confirms or refutes Wall 2 and decides whether D5 is in scope. Revert the
   throwaway immediately.
2. **After D2+D3:** assert the three LUBs of §a.5 directly in an
   `InferenceKernelTest` unit (`{Nil[1]->Integer[1..3]}`, `{Nil[*]->Any[*]}`,
   `{Nil[1],String[1]->String[1]}`). These are the *only* published witnesses of
   the rule; they must be pinned, not inferred.
3. **After D5:** run the three rows plus **all six** match twins
   (`testMatchWithFunctions`, `…ManyMatch`, `testMatchWithExtraParam`, and
   `testMatch`, `testMatchOneWith*`, `testMatchManyWithMany`) —
   `-Dchb.only=match`. None may regress.
4. **Widening census:** run the full Essential + Relation + Standard +
   Unclassified Channel B lane and diff the PASS/FAIL sets byte-for-byte against
   the pre-change run. Look specifically for a row that *changes answer* rather
   than *changes red to green* — that would be the arity-mismatch⇒`Any` branch
   firing unintentionally (Trap 13).
5. **Inliner/lowering probe (Trap 11):** confirm the dead `let lambdas` neither
   seeds (`SeedableLets` trial lowering) nor reaches `Lowerer`.
6. **`Var` census (D4):** count occurrences of a `Multiplicity.Var` inside a
   `Type.FunctionType` slot reaching `commonSupertype` across the whole PCT +
   corpus lane.
7. **Guard files:** `mvn -o -pl core test -Dtest=CodeShapeGuardrailTest` after the
   edit; confirm `InferenceKernelTest.commonSupertype_mergesRelationsAndIsLoudOnMismatch`
   (`:217–230`) is still green **without being edited**.

---

## OPEN QUESTIONS

1. **Does Wall 2 actually fire?** Read from source and the twin/`let` partition
   is decisive circumstantial evidence, but it was not executed. Probe 1 settles
   it in one run. *Everything in D5 is conditional on that observation.*
2. **B or D′ for D5?** B is smaller; D′ is what the reference semantically does
   (dispatch on a function *value*). Which do the *other* remaining Essential
   rows want? Leg 4 (function references as values) and Leg 5 (comparator lambdas
   in variables) look like they may want the same channel. Worth one grep across
   the 21 winnable before choosing.
3. **Should the arity-mismatch ⇒ `Any` branch (`GenericType.java:1375–1379`)
   land now?** It is the engine's cited rule, so emitting it is conformance — but
   it is the only branch that manufactures a widening, and **no witness row
   exercises it**. Land it with the citation, or leave it loud until a witness
   demands it? Leaning *land it* (a wall the engine does not have is also an
   invention), but this is a judgement call that belongs to the burn.
4. **`Multiplicity.Var`: adopt the reference's `[0..*]`, or keep our throw?**
   (D4.) Depends entirely on probe 6.
5. **Do we ever want the reference's `FunctionType`-vs-non-`FunctionType` ⇒
   `Any` (`Support.java:411–415`)?** Adopting it would contradict a deliberate
   audit pin (`InferenceKernelTest.java:229`) and could mask the D35
   relation-branch family (`MASTER.md:115`). Recommend *no* for this leg, but the
   divergence from the spec is now on the record and someone eventually has to
   adjudicate it.
6. **The pairwise-vs-n-ary fold at `Typer.java:2473`.** Equivalent for these
   three rows; **UNVERIFIED** in general over our lattice. Is there a witness
   anywhere in the corpus where fold order changes a collection's element type?
   Not censused.
7. **Spelling divergence.** We type a bare lambda as a bare `Type.FunctionType`
   (`Typer.java:257–261`); the reference types it as `LambdaFunction<{…}>`
   (`Instance.java:84–88`). D1 makes this invisible to `commonSupertype`, but our
   LUB's *result* is a bare `FunctionType` where the reference's is
   `LambdaFunction<FT>`. Is any consumer sensitive to the carrier? None found,
   but consumers of a collection-of-functions element type were not exhaustively
   audited.
