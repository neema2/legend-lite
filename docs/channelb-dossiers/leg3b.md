# Leg 3b — the `deactivate` platform function

**Row:** `testMatchWithMixedReturnType`.
**Charted error:** `unknown function 'deactivate'` — unported platform function.

> **HEADLINE.** `deactivate` is **quotation, not evaluation** — it hands back its
> argument's own un-evaluated AST node. It passes C1.6's litmus test
> unconditionally, so it is a **legitimate Clause 2b platform native and a
> straightforward port — NOT a NOT_IMPLEMENTABLE ledger row.**
>
> **But there is a second wall the charter never saw:** even with `deactivate`
> perfectly ported, `MatchChecker`'s narrowing makes `$z.genericType.rawType`
> yield `Integer`, not `Any`, and the row still fails. See §b.5.

*All `/Users/neemsandv/…` citations are the authoritative reference (legend-pure
@ `d00cfd5ba`, master, clean — `git status --porcelain` empty, verified). All
`/Users/neema/legend/legend-lite/…` citations are the ACTIVE tree, read-only.*
See `README.md` for the shared tenet quick-reference.

---

## a) REFERENCE SEMANTICS

### a.1 The test, verbatim

`/Users/neemsandv/legend/legend-pure/legend-pure-core/legend-pure-m3-core/src/main/resources/platform/pure/essential/lang/flow/match.pure:182-186`

```
function <<PCT.test>> meta::pure::functions::lang::tests::match::testMatchWithMixedReturnType<Z|y>(f:Function<{Function<{->Z[y]}>[1]->Z[y]}>[1]):Boolean[1]
{
    let z = $f->eval(|^LA_Location(place='Hoboken, NJ', type=LA_GeographicEntityType.CITY)->match([a:LA_Address[1] | 'address', l:LA_Location[1] | 1, a:Any[1] | 'Any1'])->deactivate());
    assertIs(Any, $z.genericType.rawType->toOne());
}
```

The model (`.../platform/pure/grammar/functions/lang/_testModel.pure`):

- `LA_GeographicEntity { type : LA_GeographicEntityType[1]; }` — **:17-20**
- `LA_Location extends LA_GeographicEntity { place:String[1]; censusdate:Date[0..1]; }` — **:22-26**
- `LA_Address extends LA_GeographicEntity { name:String[1]; }` — **:28-31**
- `Enum LA_GeographicEntityType { CITY, COUNTRY, REGION }` — **:71-76**

`match.pure` imports **only** `meta::pure::test::pct::*`,
`…lang::tests::match::*`, `…lang::tests::model::*` (**match.pure:15-17**) — **no
`meta::pure::functions::meta::*` import**, so `deactivate` is written bare. That
is why Channel B reports `unknown function 'deactivate'` while Channel A
(wire-serialized, FQN-resolved) reports
`unknown function 'meta::pure::functions::meta::deactivate'`.

**What the test asserts:** `LA_Location` is the receiver; the three handlers
return `String`, `Integer`, `String`. Pure's
`match<T|m,n>(var:Any[*], functions:Function<{Nil[n]->T[m]}>[1..*]):T[m]`
(**match.pure:20-25**) binds `T` to the join of all three handler return types =
`Any`. `deactivate` hands back the `match(...)` call's own AST node;
`.genericType.rawType` is that node's **statically inferred return type**. So the
test is a **pure type-inference assertion**: *`match` with mixed handler return
types infers `Any`.* No value is ever computed.

### a.2 Declaration — exactly ONE, no overloads

`/Users/neemsandv/legend/legend-pure/…/platform/pure/essential/meta/reflect/deactivate.pure:19`

```
native function <<PCT.function, PCT.platformOnly>> meta::pure::functions::meta::deactivate(var:Any[*]):ValueSpecification[1];
```

- Param `var : Any[*]`. Return `ValueSpecification[1]`.
- Stereotypes `PCT.function` **and `PCT.platformOnly`** — the same marker carried
  by `new`/`copy`/`extractEnumValue`/`newMap`/`groupBy`/`debug`
  (`.../lang/creation/new.pure:19`, `.../lang/creation/copy.pure:27`,
  `.../lang/enum/extractEnumValue.pure:27`,
  `.../collection/anonymous/map/newMap.pure:17`,
  `.../collection/anonymous/map/groupBy.pure:18`, `.../tools/debug/debug.pure:17`).
- Exhaustive grep over legend-pure (excluding `.git`/`target`) for
  `deactivate|Deactivate|DEACTIVATE`: **126 hits across 25 files**. Exactly one
  is a declaration of this function. No overload exists anywhere.

### a.3 Implementations — exactly TWO, both read end to end

**(i) Interpreted** —
`/Users/neemsandv/legend/legend-pure/legend-pure-runtime/legend-pure-runtime-java-engine-interpreted/src/main/java/org/finos/legend/pure/runtime/java/interpreted/natives/essentials/meta/reflect/Deactivate.java`
(51 lines, whole file read). The entire body:

```java
:41  public CoreInstance execute(ListIterable<? extends CoreInstance> params, …)
:43      return ValueSpecificationBootstrap.wrapValueSpecification(params.get(0), false, processorSupport);
:47  public boolean deferParameterExecution() { return true; }   // :47-50
```

Two branches total. `deferParameterExecution() == true` is the whole semantics.

**(ii) Compiled / code-generating** —
`/Users/neemsandv/legend/legend-pure/legend-pure-runtime/legend-pure-runtime-java-engine-compiled/src/main/java/org/finos/legend/pure/runtime/java/compiled/generation/processors/natives/essentials/meta/reflect/Deactivate.java`
(44 lines, whole file read):

```java
:31   super("deactivate_Any_MANY__ValueSpecification_1_");
:38   ListIterable<? extends CoreInstance> parametersValues = Instance.getValueForMetaPropertyToManyResolved(functionExpression, M3Properties.parametersValues, processorSupport);
:40   CoreInstance valueSpecification = parametersValues.get(0);
:41   String type = TypeProcessor.javaInterfaceForType(processorSupport.getClassifier(valueSpecification), processorSupport);
:42   return "((" + type + ")((CompiledExecutionSupport)es).getMetadata(\"" + MetadataJavaPaths.buildMetadataKeyFromType(…) + "\",\"" + processorContext.getIdBuilder().buildId(valueSpecification) + "\"))";
```

No branches. `transformedParams` (the generated code for the argument) is passed
in and **discarded**.

Registrations — exactly two, located exhaustively:

- interpreted: `FunctionExecutionInterpreted.java:531` —
  `nativeFunctions.put("deactivate_Any_MANY__ValueSpecification_1_", new Deactivate(this, repository))`
- compiled: `NativeFunctionProcessor.java:497` — `registerNative(map, new Deactivate())`

No third implementation exists in legend-pure, and **none at all in
legend-engine** (exhaustive grep excluding `.git`/`target`/`node_modules`: 48
non-`evaluateAndDeactivate` hits; every one a `.pure` call site, a manifest
string, or the Elasticsearch `watcher.deactivate_watch` schema — no Java class).

### a.4 What `deactivate` actually does — established from the bodies

**The interpreted path.** `FunctionExpressionExecutor.java:73-81` (whole file read):

```java
:73  boolean deferExecution = Instance.instanceOf(function, M3Paths.NativeFunction, …) && …getNativeFunction(…).deferParameterExecution();
:75  MutableList<CoreInstance> parameters = (deferExecution || params.isEmpty())
:76          ? Lists.mutable.withAll(params)                       // ← RAW AST NODES
:77          : params.collect(p -> { Executor executor = …; return executor.execute(p, …); });
```

with `params` = `functionExpression._parametersValues()` (**:67**). So with
`deferParameterExecution()==true`, `Deactivate.execute` receives **the argument's
un-evaluated `ValueSpecification` node exactly as it sits in the compiled
metamodel graph**. Only 4 natives in all of legend-pure defer (exhaustive grep on
`deferParameterExecution`): `And`, `Or`, `ToMultiplicity`/`CommonToMultiplicity`,
and `Deactivate`.

`ValueSpecificationBootstrap.wrapValueSpecification(CoreInstance, boolean, ProcessorSupport)`
(`.../m3/navigation/ValueSpecificationBootstrap.java:39-42`, whole file read):

```java
:41  return (ValueSpecification.isNonExecutableValueSpecification(value, ps) || Measure.isUnitOrMeasureInstance(value, ps)) ? value : wrap(value, getTypeForWrapping(executable), ps);
:111 private static String getTypeForWrapping(boolean executable) { return executable ? M3Paths.InstanceValue : M3Paths.NonExecutableValueSpecification; }
:149 private static CoreInstance wrap(CoreInstance value, String type, ProcessorSupport ps) {  // :149-163
:157     CoreInstance inst = ps.newEphemeralAnonymousCoreInstance(type);
:158     Instance.addValueToProperty(inst, M3Properties.values, value, ps);
:159     Instance.addValueToProperty(inst, M3Properties.genericType, GenericType.copyGenericType(Instance.extractGenericTypeFromInstance(value, ps), ps), ps);
:160     Instance.addValueToProperty(inst, M3Properties.multiplicity, ps.package_getByUserPath(M3Paths.PureOne), ps);
```

`executable=false` ⇒ the wrapper is a **`NonExecutableValueSpecification`** with
`values=[the AST node]`, `multiplicity=PureOne`.
`isNonExecutableValueSpecification` is `true` only for that exact class
(`.../navigation/valuespecification/ValueSpecification.java:66-77`, whole file
read) — so a `VariableExpression`/`InstanceValue`/`SimpleFunctionExpression`
argument always takes the `wrap` path. The Pure-level value is the wrapper's
single `values` element = **the argument's AST node**.

**Corroborated by the fixture, per input shape** (`deactivate.pure`, all 15 call
sites read):

| Input shape | Line | AST node returned | Assertion |
|---|---|---|---|
| `$a` where `let a = 5` | :35-40 | `VariableExpression` | `$b->cast(@VariableExpression).name == 'a'` — **the value 5 is never computed** |
| `[$a, $b]` | :42-48 | `InstanceValue` holding two `VariableExpression`s | `'a,b'` |
| `…->filter(…)->first()` | :21-26 | `SimpleFunctionExpression` | `.func.functionName == 'first'`, `parametersValues->size() == 1` |
| `…->filter(…)` | :28-33 | `SimpleFunctionExpression` | `.func.functionName == 'filter'`, 2 params |
| `42` / `'hello'` / `true` | :89-108 | `InstanceValue` | `type()->id() == 'InstanceValue'`, `.values == [42]` etc. |
| `{\|1+2}` | :110-118 | `InstanceValue` wrapping the `LambdaFunction` | `expressionSequence->size() == 1` |
| `[1,2,3]` then `->reactivate()` | :136-139 | round-trips to `[1,2,3]` | identity |

**Verdict on semantics:** `deactivate(e)` = *"give me `e`'s own syntax tree node,
un-evaluated."* It is **quotation**, not evaluation.

**The compiled path proves this is compile-time-resolvable.**
`NativeFunctionProcessor.processNativeFunction` (**:237-250**) calls `nat.build(...)`
**during Java source generation** (**:247**), and `Deactivate.build` returns a
*constant* string embedding `processorContext.getIdBuilder().buildId(valueSpecification)`
— the node's identity computed at generation time and baked in as a string
literal. The runtime does a metadata lookup by that constant key. **No argument
evaluation, no runtime search, no walk.**

### a.5 What it returns in THIS test, and what consumes it

Argument = the `SimpleFunctionExpression` for `match(…)`. `deactivate` returns
that node. Downstream: `$z.genericType` (the M3 `ValueSpecification.genericType`
property = the node's inferred type) → `.rawType` → `->toOne()` →
`assertIs(Any, …)`. **The only property read is the static type.** No `func`, no
`parametersValues`, no `values`, no `reactivate`.

### a.6 The reference's own judgment about portability

- **No adapter in legend-engine passes this test.** Exhaustive
  `grep -A3 testMatchWithMixedReturnType` over all legend-engine manifests —
  **14 hits, every one an `expectedError`**:
  - 11 relational adapters (postgres `:463-464`, clickhouse `:466-467`, duckdb
    `:289-290`, h2 `:469-470`, sqlserver `:475-476`, databricks `:304-305`,
    spanner `:523-524`, memsql `:493-494`, trino `:475-476`, snowflake `:277-278`,
    oracle `:484-485`) all record
    `"type not supported: meta::pure::functions::lang::tests::model::LA_GeographicEntityType"`
    — they die building a relational table from the model (`pct_relational.pure:836`:
    `pureTypeToDataType(…)->toOne('type not supported: ' + …)`, over
    `LA_GeographicEntity.type : LA_GeographicEntityType[1]`), **before
    `deactivate` is ever reached**.
  - Java platform binding (`Test_JAVA_EssentialFunction_manifest.json:182`):
    `"Error in 'test::testFunction': Function does not exist 'deactivate(Any[1])'"`.
  - Deephaven (`EssentialFunctions_manifest.json:542`):
    `"Function does not exist 'meta::pure::functions::meta::deactivate(Any[1])'"`.
- `deactivate_Any_MANY__ValueSpecification_1_` is on legend-engine's
  Java-generation **prohibited list**, in the `// PURE Implementation` group,
  alongside `canReactivateDynamically`, `compileValueSpecification`,
  `evaluateAndDeactivate` —
  `.../legend-engine-xt-javaGeneration-pure/…/generation/conventions.pure:1238`
  (inside `defaultProhibitedFunctions`, `:1219`).
- `javaGenerationTest.pure:159` has the test commented out:
  `// … testMatchWithMixedReturnType…, // requires deactivate_Any_MANY__ValueSpecification_1_`.

So: **only the two reference Pure runtimes implement it**, and both do so by
handing back a compile-time-known AST node.

### a.7 Full use census (exhaustive)

**legend-pure — 26 `->deactivate()` call sites:** `deactivate.pure` :23, :30,
:38, :46, :52, :58, :65, :75, :91, :98, :105, :114, :125, :133, :138 (15);
`reactivate.pure` :27, :33, :41, :57, :62 (5); `match.pure` :184 (1);
`AbstractTestReactivate.java:185` (1); `TestGetAllValidator.java` :125, :139,
:159, :178 (4).

**legend-engine — 29 call sites:** `testPureToSql.pure` :37, :50, :61, :63, :79,
:91, :100, :108, :126, :142 (10); morphir `transform.pure` :187, :189, :191,
:193, :195 (5); `testLambda.pure` :52, :63, :64, :108 (4); `xsdToPure.pure`
:1902, :1903 (2); `testConnection.pure` :179, :180 (2); `generation.pure` :44,
:45 (2); `testDynamic.pure` :69, :70 (2); `testGenerator.pure:92`;
`lineage_fct.pure:82`; `AbstractTestCompileValueSpecification.java:161`;
`chain.pure:41`; `aggregationAware.pure:46`.

**Excluded as non-uses (verified individually):** `validationAlloy.pure:97` (a
variable named `deactivatedMapping`), `router_main.pure:59,:79` (`$deactivatedEs`
variable), `legend-pure-lsp-vscode/src/extension.ts:203` (VSCode lifecycle hook),
Elasticsearch `schema-7.17.json` (`watcher.deactivate_watch` REST API).

**Most-used downstream reads across the corpus:**
`.genericType`/`.genericType.rawType` (match.pure:185, morphir ×5,
xsdToPure:1903, chain.pure:41, aggregationAware.pure:46), `.multiplicity`
(generation.pure:44-45, testLambda.pure:52/63/64, xsdToPure:1902),
`->cast(@InstanceValue).values` (testPureToSql ×10, testLambda:108,
deactivate.pure), `->cast(@SimpleFunctionExpression).func.functionName` /
`.parametersValues` (deactivate.pure, TestGetAllValidator ×4), `->reactivate()`
(reactivate.pure, deactivate.pure).

---

## b) OUR SEAM

### b.1 The wall, exact

`core/src/main/java/com/legend/compiler/spec/Typer.java:1612-1636` (`checkGeneric`):

```java
:1616  for (ValueSpecification p : af.parameters()) { args.add(synth(p, env)); }   // args typed FIRST
:1623  List<TypedFunction> candidates = functionCandidates(af);
:1624  if (candidates.isEmpty()) {
:1628      throw new TypeInferenceException("unknown function '" + af.function() + "' — no function of this name in the native or user catalog (unported platform function, or a misspelling)");
```

**Consequence worth stating:** args are synthesized (`:1616-1618`) *before* the
candidate lookup (`:1623`). The reported error is the `deactivate` wall,
therefore **the `match(...)` argument already types successfully today.** That is
an observation, not an inference from naming.

`deactivate` is not a `CoreFn` (`CoreFn.java` enumerated in full; no arm), so it
takes the generic path. Sibling walls that are *not* hit: `Typer.java:510`,
`FunctionCompiler.java:203`.

### b.2 The registry

`core/src/main/java/com/legend/builtin/Pure.java` (2266 lines):

- `signature(String pureSignature)` — **:1090-1107**: parses a verbatim
  `native function …;` through `ElementParser`, appends to `ALL` (**:1104**).
  Declaration order is load-bearing (**:742-748**).
- `Index` static block — **:941-968**: builds `FN_BY_FQN`, `FN_BY_BARE`
  (bare-name index, excludes the `meta::legend::lite::` package except
  `LITE_SURFACE`), `KEYS_BY_NAME`.
- `nativeFunctionsAt(String)` — **:1064-1072**: FQN lookup or bare-name union.
- Golden: `core/src/test/resources/native-catalog.txt` (721 lines), asserted by
  `core/src/test/java/com/legend/builtin/NativeFunctionTest.java:59`. **A new
  native = a golden diff.**

**What is missing today, precisely:**

1. **No `deactivate` native.** Verified by full grep of `Pure.java` and
   `native-catalog.txt`.
2. **No `ValueSpecification` type at all.**
   `grep -rn "ValueSpecification" core/src/main/java/com/legend/builtin/` → empty;
   `native-catalog.txt` has no
   `valuespecification`/`InstanceValue`/`SimpleFunctionExpression`/`VariableExpression`.
   (`com.legend.protocol.spec.ValueSpecification` is legend-lite's own
   *parsed-AST* Java interface — a different thing, not a Pure-level type.)
3. **The pieces that DO exist:**
   - `native Class meta::pure::metamodel::type::generics::GenericType { rawType: meta::pure::metamodel::type::Type[0..1]; }` — **Pure.java:185**
   - `native Class meta::pure::metamodel::type::Type extends meta::pure::metamodel::ModelElement {}` — **Pure.java:182**
   - `native Class meta::pure::metamodel::type::Any {}` — **Pure.java:170**
   - `meta::pure::functions::meta::genericType(any:Any[*]):GenericType[1]` — **Pure.java:1366**; `type(any:Any[*]):Type[1]` — **:2235**; `id`, `instanceOf` — **:1549, :1457**
   - `meta::pure::functions::boolean::is(left:Any[1], right:Any[1]):Boolean[1]` — **Pure.java:1295** (comment: *"NO SQL lowering: the assertIs K-arm adjudicates identity in World 1 for statically-identified operands (type refs, folded instance provenance); any other use walls loudly at lowering"*)

### b.3 The existing compile-time-reflection precedents

**(A) `AssertVerdicts.typeIdentityOf` — the closest precedent.**
`core/src/main/java/com/legend/AssertVerdicts.java:336-366`:

```java
:339  if (s instanceof TypedPackageableRef pr) return canonicalTypeFqn(pr.fullPath());
:342  if (s instanceof TypedTypeRef tr)        return canonicalTypeFqn(tr.target().typeName());
:345  if (s instanceof TypedNativeCall c && c.callee().qualifiedName().equals("meta::pure::functions::meta::type") && !c.args().isEmpty()) return staticTypeName(c.args().get(0));
:353  if (s instanceof TypedPropertyAccess pa && pa.property().equals("rawType") && peel(pa.source()) instanceof TypedNativeCall gt && gt.callee().qualifiedName().equals("meta::pure::functions::meta::genericType") …) return staticTypeName(gt.args().get(0));
```

This already folds `type(x)` and `genericType(x).rawType` **to the argument's
static type at verdict time, with no database**. `isVerdict` (**:317-333**) then
compares the two identity strings. `assertIs` routes here at **:279-286**.

**(B) `resolver/GenericTypeReflection.java`** (130 lines, whole file read) —
`genericType(<class extent>).rawType` over ROWS, resolved **by emission** as a
membership-witness `CASE` (`rawTypeProjection`, **:70-124**), with
`NotImplementedException` on unrecognized shapes (**:74, :97**). Different shape
(per-row runtime identity), same doctrine.

**(C) `Pure.java:1645-1656` — the declared precedent for exactly this situation:**

> `concatenateTemporalTdsQueries` … *"its real body folds the queries into
> concatenate SimpleFunctionExpressions — **reflection metamodel this platform
> lacks**, so the corpus copy is signature-broken and drops at overload
> collection. This native carries the TYPE; the harness splices the SAME
> semantics by EMISSION."*

**(D) `evaluateAndDeactivate`** — `Pure.java:1585-1589`, registered verbatim as
`<T|m>(var:T[m]):T[m]` with the note *"values here are already values, so it is
the identity"*; erased in `lowering/Scalars.java:430-433`, peeled in
`compiler/spec/ExecuteChainAssembly.java:129-133,:164`, and shape-matched in
`Typer.java:1878,:1883`. **`deactivate` is NOT the identity** and must not be
modeled on this.

### b.4 How the row is scored

- **Channel A** (`pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_EssentialFunctions_PCT.java:118-121`)
  — the row is **already an expectedFailure with today's exact text pinned**:

```java
// deactivate() reflects the EXPRESSION (a ValueSpecification metamodel
// object) — legend-lite compiles to SQL and holds no expression tree at
// run time; metamodel reflection is out of vocabulary.
one("meta::pure::functions::lang::tests::match::testMatchWithMixedReturnType_Function_1__Boolean_1_", "\"unknown function 'meta::pure::functions::meta::deactivate' — no function of this name in the native or user catalog (unported platform function, or a misspelling)\""));
```

  Per the file's own header (**:33-35**), pins are *"contains-matched, so any
  regression that changes the failure shape — **or a fix that makes one pass** —
  fails loudly."* **Porting `deactivate` REQUIRES deleting this pin in the same
  commit**, or Channel A goes red.
- **Channel B** (`pct/.../channelb/ChannelB.java`, 459 lines, whole file read)
  does **not** read `expectedError` at all — `runOneInner` (**:216-249**) returns
  PASS / FAIL / DECLINED / ERROR from actually running the body under the
  identity adapter (`eliminateAdapter`, **:271-316**). Today: **ERROR**.
- `ChannelBEssentialTest.java`: `channelAExpectedFailures()` (**:267-282**)
  scrapes the `one("…")` names, so this row currently counts **AGREE-FAIL**;
  `engineDuckDbExclusions()` (**:244-261**) scrapes *every* test named in
  `pct/src/test/resources/oracle/EssentialFunctions_manifest.duckdb.json` (this
  row at **:289-290**), so it also counts **ENGINE-FRONTIER**, never
  TRUE-WIRE-BUG. **Note the frontier bucket is *why* the row is currently
  invisible — it is not evidence that it should stay failing.**
- **The charter puts it in the winnable set, explicitly:**
  `docs/CHANNELB_BURNDOWN_HANDOFF.md:52`, under *"The winnable 21 (Essential)"*
  (**:45**), with the mission *"burn Channel B PCT to 100% modulo the nine
  adjudicated ledger rows"* (**:3-5**). This row is **not** one of the nine
  (**:32-43**).

### b.5 ⚠️ THE SECOND WALL — the one nobody has seen yet

**`deactivate` is not the only thing standing between this row and green.** Read
`core/src/main/java/com/legend/compiler/spec/MatchChecker.java` (309 lines, whole
file read) against this test's exact inputs:

- `optionalRuntimeDispatch` (**:198-267**): requires input multiplicity `[0..1]`
  (**:203-206**). Ours is `[1]` → **returns null**.
- `runtimeMatch` (**:120-181**): `narrows` is true only when a branch type is a
  **strict subtype** of the input type (**:130-132**, with `accepts(formal, actual)`
  ≡ *actual conforms to formal*, `InferenceKernel.java:1197-1199, 1203-1263`).
  Input `LA_Location`; branches `LA_Address` (sibling, not a subtype),
  `LA_Location` (equal, not strict), `Any` (supertype). → `narrows == false` →
  **returns null**.
- Static path (**:74-113**): first accepting branch. `LA_Address` rejects
  (`LA_Location` ⊀ `LA_Address`); **`l:LA_Location[1] | 1` accepts** → result node
  is `TypedMatch(..., body=1, info = body.info())` = **`Integer[1]`**.

And this is **deliberate and documented** — `MatchChecker.java:22-24`:

> *"the result is the body's type — **not the registered signature's `Any[*]`
> collapse**."*

The LUB path exists but is only reachable through `runtimeMatch` (**:168-171**,
`commonSupertype`), which this test does not enter.
`InferenceKernel.commonSupertype(String, Integer)` **would** give `Any`
(`:1285-1305`: neither is a subtype of the other → nearest shared ancestor →
`anyType()`, **:1377-1379**).

**So even with `deactivate` perfectly ported, `$z.genericType.rawType` yields
`Integer`, and the row fails `expected: …::Any / actual: …::Integer`. The leg has
two independent halves.**

> **Cross-leg note.** Leg 3 independently found a *different* second wall in the
> same class — `MatchChecker.branches` (`:299-308`) cannot see branch lambdas
> behind a `let` variable. `MatchChecker` is thus the hidden blocker for **four**
> of the charter's winnable rows (3's three plus this one). See `leg3.md` §b.2.

---

## c) MINIMUM DESIGN — the decisions

### C1.6 litmus test, answered from the implementation body

> *"could this run with no database attached and no data loaded?"* — `TENET_CHARTER.md:35`

**Yes, unconditionally.** Evidence, not assertion:

- Interpreted: `deferParameterExecution()==true` ⇒ `FunctionExpressionExecutor.java:75-76`
  hands over the **raw AST node**; `Deactivate.java:43` wraps it. No executor
  runs, no value exists.
- Compiled: `Deactivate.build` (**:35-43**) runs at **code-generation time**
  (`NativeFunctionProcessor.java:247`) and emits a **constant metadata key**.
  `transformedParams` is discarded.
- The test's only downstream read is `.genericType.rawType` — the node's
  statically inferred type.

**`deactivate` never touches stored data, a `ResultSet`, or a runtime value. Its
entire input is the caller's own syntax tree; its entire output is a model-space
fact.** That is C1.6 model-space computation verbatim, and therefore Clause 2b's
*"metamodel operations"* grant (`TENET_CHARTER.md:66`) applies squarely.

**The shadow-evaluator ban does not reach here.** `NOT_IMPLEMENTABLE.md:79-86`
bans *"interpreting the engine's own compiler as data"* — tests that call
`routeFunction`/`toSQLQuery`/`buildJoinTreeNode`. `deactivate` reads a type off
the frontend's own typed HIR; legend-lite's frontend is *"the single source of
truth for types"* (AGENTS.md:153). **Leg 3b is a port, not a ledger entry.**

### The decisions

**D1 — Port it, do not ledger it.** Clause 2b, all three conditions satisfiable:
(1) ONE owner in `com.legend` on the compiled surface; (2) spec =
`deactivate.pure:19` + the two reference bodies cited above; (3) registered in
`JavaEvalLedgerTest`'s size register.

**D2 — `deactivate` is COMPILE-TIME, resolved during TYPE (phase G). It never
reaches MIR.** Forced by AGENTS.md invariant 3a (**:205-227**): *"MIR never holds
a Pure AST node"* and *"No `FunctionCall(String, args)` catch-all… New native =
new MIR variant + new render arm."* Since `deactivate` produces a model-space
fact, **it must be fully folded away before lowering** — so it costs **zero** MIR
variants and **zero** render arms. This is the single most important structural
decision in the leg: it is what keeps `Lowerer.java` and every dialect untouched.

**D3 — Register the real signature verbatim, plus the minimum metamodel type.**
`Pure.java`, citing `deactivate.pure:19`:

```
native function meta::pure::functions::meta::deactivate(var:meta::pure::metamodel::type::Any[*]):meta::pure::metamodel::valuespecification::ValueSpecification[1];
```

`ValueSpecification` must be added as a `nativeClass(...)` beside `GenericType`
(**Pure.java:185**), carrying **only** `genericType : GenericType[1]`. Every other
M3 property (`values`, `func`, `parametersValues`, `multiplicity`, `name`) is
**deliberately absent** so that any other use walls at property resolution rather
than fabricating. Goldens: `native-catalog.txt` regenerates
(`NativeFunctionTest.java:59`).

**D4 — Fold `deactivate(e).genericType.rawType` to a `TypedTypeRef` at TYPE time.**
`TypedTypeRef(Type target, ExprType info)` already exists
(`compiler/spec/typed/TypedTypeRef.java`) and `AssertVerdicts.typeIdentityOf:342`
already reads it: `canonicalTypeFqn(tr.target().typeName())`, with
`Type.ClassType.typeName()` returning the FQN
(`compiler/element/type/Type.java:255-264`). The left operand `Any` arrives as a
`TypedPackageableRef` and is read at **:339**. **So the verdict half needs no new
comparison logic — only the fold.** Two sub-shapes, both compile-time:

- `deactivate(e)` → a node carrying `e`'s **declared** type (see D5) at Pure type
  `ValueSpecification[1]`;
- `.genericType` → `GenericType[1]` carrying the same; `.rawType` →
  `TypedTypeRef(thatType)`.

**D5 — The declared-vs-selected type divergence gets NAMED, not papered over.**
`MatchChecker`'s narrowing (`:22-24`, `:111-112`) is correct for emission and
must not change — it is load-bearing across the whole match family. But
`deactivate` must report what **Pure's signature** says the expression's type is,
not what our emission narrowed it to. The decision:
**`TypedMatch`/`TypedMatchRuntime` carry a second, declared type — the branch-body
LUB via the kernel's existing `commonSupertype`** (`InferenceKernel.java:1238`,
already used at `MatchChecker.java:168-169`) — and `deactivate` reads *that*.
`info()` (emission) is untouched; the new accessor is read by exactly one caller.
This is the honest form: two types, one node, each with a stated owner. *(A
`deactivate`-side special case that re-types `match` would be a second
implementation of match typing — banned by Clause 2b's closing sentence,
`TENET_CHARTER.md:74-76`.)*

**D6 — Absence is a loud wall, by construction.** Because D3 registers only
`genericType`, every unported shape in the corpus
(`->cast(@SimpleFunctionExpression).func.functionName`, `.parametersValues`,
`->cast(@InstanceValue).values`, `.multiplicity`, `->reactivate()` — §a.7) fails
at **property/cast resolution with the property's own name**, not at a stub. That
satisfies AGENTS.md invariant 4 (**:244-252**) and Clause C2.4 (**:52-55**). **No
`deactivate` node may ever produce a plausible-looking default.**

**D7 — Same-commit pin work (non-negotiable, or the build goes red):**

- delete `Test_LegendLite_EssentialFunctions_PCT.java:118-121` (contains-matched
  pin, header **:33-35**);
- bank `pass >= 305` → `306` and `agreePass`/`bFixesA` in
  `ChannelBEssentialTest.java:78,:164` with a dated justification (handoff §4, **:166**);
- regenerate `native-catalog.txt`;
- if the fold lands in `AssertVerdicts`, bump `JavaEvalLedgerTest.java`'s
  `AssertVerdicts.java` row (currently `834` stripped lines) with a written
  justification. **Prefer landing it in the Typer/checker (not ledgered) over
  `AssertVerdicts` (ledgered)** — cheaper on pins and truer to phase G.

---

## d) TRAPS

1. **`Lowerer.java` is EXACTLY at 3500** (`wc -l` = 3500;
   `CodeShapeGuardrailTest.java:35 FILE_LIMIT = 3500`; `FILE_ALLOWLIST` at
   **:42-46** contains only `MappingNormalizer.java`). **D2 is what keeps this leg
   out of `Lowerer` entirely.** If a design draft finds itself adding a lowering
   arm, the design is wrong, not the guardrail. Neighbours also near the ceiling:
   `StoreResolver` 3464, `Scalars` 3424, `Typer` 3194, `StatementExecutor` 3089.

2. **The stub trap — the exact NO-FALLBACKS violation.** Registering `deactivate`
   and returning *anything* generic (identity, like `evaluateAndDeactivate`; or a
   `ValueSpecification` with fabricated properties) would make
   `$z.genericType.rawType` produce a plausible type and could make the row **pass
   for the wrong reason**. `evaluateAndDeactivate` is identity *because pure says
   so* (`evaluateAndDeactivate.pure:17`, `<T|m>(var:T[m]):T[m]`); `deactivate`
   returns `ValueSpecification[1]` and is **not** identity. Copying the
   `evaluateAndDeactivate` treatment is the single most likely wrong move.

3. **The hidden second wall (§b.5).** A burn session that ports `deactivate` and
   re-runs will get `expected: Any / actual: Integer` and may conclude the port is
   broken. It is not — `MatchChecker` narrowing is doing exactly what it
   documents. **Diagnose D5 before touching the port.**

4. **Record-equality / inliner hazards.** `TypedTypeRef` is a record with `equals`
   over `(target, info)`; `TypedMatch` is a record whose identity would change if
   D5 adds a component. `UserCallInliner.inlineBody` (G½, *"runs on every
   execution path"* — AGENTS.md:89) and `SourceSubst`/`Typer.alphaRename`
   (**Typer.java:100-145**) walk and rebuild typed trees with `withChildren`.
   `TypedTypeRef.withChildren` asserts **0 children** and returns `this`. Any new
   node must implement `children()`/`withChildren` consistently or the inliner
   will silently drop the carried type. Adding a component to `TypedMatch` changes
   its `withChildren` arity contract — check every construction site.

5. **Stale charter assumptions to distrust:**
   - `docs/FUNCTION_REGISTRY.md` carries an explicit era banner (**:1-7**):
     *"Written against `engine/com.gs.legend`, which is frozen… `PureModelBuilder`,
     `TypeChecker`, `MappingResolver`, `SqlAggregate`, `SqlRelation`, `SQLDialect`
     — do not exist in the live tree."* Its *mechanism* (verbatim Pure signature
     strings) is live in `Pure.java:1090`; its *class names* are dead.
   - `CHANNELB_BURNDOWN_HANDOFF.md:29` says Essential 297;
     `ChannelBEssentialTest.java:78` pins ≥305 (legs 1-2 landed after). **Trust the
     test.**
   - The Channel A comment at `Test_LegendLite_EssentialFunctions_PCT.java:118-120`
     (*"metamodel reflection is out of vocabulary"*) is an in-line **ledger note,
     not an adjudication**. It predates Clause 2b (ratified 2026-08-18,
     `TENET_CHARTER.md:61`) and C1.6, and the burndown charter (`:52`) supersedes
     it by listing the row as winnable. Do not cite it as the reason to stop.

6. **Bare-name index pollution.** `Pure.java:941-968` puts every non-`lite` native
   into `FN_BY_BARE`. `deactivate` has no existing homonym (verified across
   `Pure.java` and `native-catalog.txt`), so this is low-risk — but
   `ValueSpecification` as a new native **class** enters `nativeClassFqns()`
   (**:1074-1077**) and the NameResolver prelude. Check no corpus element shadows it.

7. **`assertIs` is `PCT.platformOnly` with no SQL lowering** (`Pure.java:1290-1295`).
   Anything that pushes this comparison toward SQL is off-charter; the World-1
   verdict route (`AssertVerdicts:279-286`) is the chartered one (Clause 2c,
   `TENET_CHARTER.md:78-92`).

8. **`PctDisciplineTest`** (`pct/src/test/java/.../PctDisciplineTest.java:29-35`)
   bans `sort`/`distinct`/`abs(<`/`TreeSet`… anywhere under `pct/src`. Keep all of
   this in `core`.

---

## e) CONFIDENCE + LIVE PROBES

**High confidence (read end-to-end from source, every branch):**

- `deactivate`'s semantics = un-evaluated argument AST node, wrapped
  `NonExecutableValueSpecification`. Both implementations read in full;
  `deferParameterExecution` traced to its only consumer.
- **It is NOT runtime-reflective.** The compiled backend resolves it to a
  constant at code-generation time. It passes C1.6's litmus test unconditionally.
  **Leg 3b is XS-to-M as a port, not XL, and not NOT_IMPLEMENTABLE.**
- Exactly one declaration, no overloads, two implementations, two registrations,
  55 call sites across both reference repos — all enumerated.
- No legend-engine adapter passes the test; it is on the Java-generation
  prohibited list.
- legend-lite has no `ValueSpecification` type; `GenericType{rawType}`, `Type`,
  `Any`, `TypedTypeRef` and the `assertIs` identity verdict all exist.
- `Lowerer.java` = 3500 = `FILE_LIMIT`, no allowlist entry.
- Channel A pins today's exact `unknown function` text and will go red on a fix.

**Medium confidence (read, not executed):**

- **The `MatchChecker` narrowing prediction (§b.5).** Derived by hand-tracing
  `optionalRuntimeDispatch` → `runtimeMatch` → static loop with `accepts`
  semantics from `InferenceKernel.java:1197-1263`. High-value, so **probe it first**.
- That `staticTypeName` (`AssertVerdicts:367-382`) returns `…::Any` for an
  `Any`-typed argument. The code returns `ct.fqn()` for any `Type.ClassType` and
  `anyType()` **is** a `ClassType` — but the method's own doc says *"never an
  Any/generic stamp"*. Code and comment disagree; the code path is what runs.

**Explicitly UNVERIFIED:**

- **The verbatim readable `Class …valuespecification::ValueSpecification { … }`
  M3 declaration.** legend-pure's M3 is bootstrapped from a serialized M4 graph
  (`platform/pure/grammar/m3.pure` — 35 lines mention
  `children[valuespecification].children[ValueSpecification]`, all in
  `Root.children[…]` graph form, not class syntax). Searched `m3.pure`, all
  legend-pure `.pure` resources, and all of legend-engine for
  `Class meta::pure::metamodel::valuespecification::ValueSpecification` /
  `SimpleFunctionExpression extends` — the only hit is an unrelated
  `StoreValueSpecificationContext` (`legend-pure-m2-dsl-store-pure/…/store.pure:42`).
  The generated CoreInstance interfaces live under `target/` (absent). **What
  would settle it:** the generated
  `org.finos.legend.pure.m3.coreinstance.meta.pure.metamodel.valuespecification.ValueSpecification`
  interface after a legend-pure build, or the m3 graph decoded. Until then,
  `genericType`, `multiplicity`, `values` as ValueSpecification-level properties
  are established from *usage* (`ValueSpecificationBootstrap.java:158-160` writes
  exactly those three; `ValueSpecification.java:126-140` reads `values`) rather
  than from a declaration.
- Whether `let z = …` is inlined before `AssertVerdicts.isVerdict` sees it. `Env`
  carries `ExprType` only (`MatchChecker.java:97`), and `isVerdict` (**:317-333**)
  takes no `letPrefix` — so `$z` would arrive as a `TypedVariable` that `peel`
  (**:404-415**) does not resolve. `letPrefix` **does** accumulate `TypedLet`
  nodes carrying their values (`StatementExecutor.java:179`), so a structural,
  DB-free resolution is available (`instanceOrigin`, **:417-427**, already does
  the analogous fold for `TypedNewInstance`) — but **which fold site is correct is
  the open design question**, and it is exactly where the leg could quietly grow.
- Today's precise Channel B outcome string for this row (nothing was run).

### Probes for the burn session, in this order

1. **`-Dchb.only=testMatchWithMixedReturnType`** on `ChannelBEssentialTest` —
   capture the exact ERROR string and confirm the bare-name spelling.
2. **The narrowing probe (do this before writing any code).** Type
   `^LA_Location(…)->match([a:LA_Address[1]|'address', l:LA_Location[1]|1, a:Any[1]|'Any1'])`
   and print the resulting node class + `info().type()`. If it is `TypedMatch` /
   `Integer[1]`, §b.5 is confirmed and **D5 is mandatory**. If it is
   `TypedMatchRuntime` / `Any[1]`, D5 collapses and the leg is a straight port.
3. **The verdict probe.** Confirm `assertIs(Any, X)` reaches `AssertVerdicts:279`
   and that `typeIdentityOf` on a `TypedTypeRef(ClassType("meta::pure::metamodel::type::Any"))`
   and on the bare `Any` element ref both yield `meta::pure::metamodel::type::Any`.
4. **The let-resolution probe.** Determine whether `$z` arrives at the verdict as
   a `TypedVariable`. This decides whether the fold belongs in phase G (preferred
   — unledgered) or in `AssertVerdicts` (ledgered, pin bump).
5. **The wall probe (proves D6, not just claims it).** After the port, compile
   `…->deactivate()->cast(@SimpleFunctionExpression).func.functionName` and
   `…->deactivate().multiplicity` and confirm both raise loudly naming the missing
   property/type. *Per the standing rule that a green check proving only
   well-formedness will pass a placeholder: **the wall probe is what distinguishes
   this port from a stub**, and it must be run.*
6. **Regression sweep:** the whole `match` family (`testMatchWithFunctions*`,
   `testMatchZeroWith*`, `testMatch`, `testMatchOneWith*`) after D5, since it
   touches `TypedMatch`'s shape.

---

## OPEN QUESTIONS

1. **Where does the fold live?** Phase G (a `CoreFn.DEACTIVATE` arm / small
   checker, unledgered, keeps typing in the Typer per invariant 1) versus
   `AssertVerdicts` (World-1, ledgered at 834 stripped lines, needs a pin bump)?
   Probe 4 decides. Reading favours phase G: the value being computed is a
   **type**, and AGENTS.md:153 makes the frontend the sole owner of types.
2. **Is D5's second type on `TypedMatch` the right shape, or should `deactivate`
   read a `declaredType()` accessor added to `TypedSpec` generally?** The general
   form is more useful (every corpus `deactivate().genericType` would then work)
   but is a much larger surface. Needs the burn session's judgment against how
   many other rows would benefit.
3. **Does `TypedMatchRuntime` need the same declared type?** Its `info()` is
   already the LUB (`MatchChecker.java:168-176`), so possibly not — but the two
   nodes should agree on the contract, not diverge by accident.
4. **Exact verbatim M3 `ValueSpecification` declaration** (see UNVERIFIED). Clause
   2b condition (2) — *"the `.pure` source is the SPEC it is ported from and
   verified against"* — wants the real text in the `nativeClass(...)` call. Settle
   it before writing the registration line.
5. **Should the port also carry `.multiplicity`?** It is the second-most-read
   property in the corpus (§a.7) and is equally compile-time. Adding it costs
   little and unlocks future rows — but it is **not** needed for this test, and
   D6's minimality is what makes the walls honest. Scope call for the burn session.
6. **Does anything else in the Essential suite consume `deactivate`?** The 26
   legend-pure call sites were verified; only `match.pure:184` is under the
   `essential/` Channel B scope *besides* `essential/meta/reflect/deactivate.pure`
   and `reactivate.pure` — whose functions are `<<test.Test>>`, not `<<PCT.test>>`,
   so `ChannelB.isPctTest` (**:181-186**) skips them. Worth confirming the
   discovery count does not move (`out.size() == 327`, `ChannelBEssentialTest.java:58`).
