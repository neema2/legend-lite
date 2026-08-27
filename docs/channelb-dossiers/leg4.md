# Leg 4 — named-function references used as VALUES

**Rows:** `testContainsWithFunction`, `testRemoveDuplicatesPrimitiveStandardFunctionExplicit`.
**Charted error:** `'comparator_…'/'cmp_…' is not a known class, mapping, runtime…`

> **HEADLINE (verified; it overturns the charter §3 hypothesis).** The
> function-reference-as-value machinery **ALREADY EXISTS** in legend-lite and is
> pinned green for FQN-qualified spellings. Both rows fail for one narrow
> reason: a **BARE (unqualified) mangled function id is never qualified to an
> FQN by `NameResolver`**, because the mangled id is not an element name in our
> model. **Channel A already passes both rows and needs nothing** — contradicting
> the charter's "Channel B burns the matching Channel A rows for free".

See `README.md` for the shared tenet quick-reference and provenance notes.

---

## a) REFERENCE SEMANTICS

### a.1 The two tests, verbatim, and the exact syntax

**`testContainsWithFunction`** —
`legend-pure-core/legend-pure-m3-core/src/main/resources/platform/pure/essential/collection/boolean/contains.pure:72-85`.
The referenced function is `comparator` in the same file, **lines 66-69**:

```
function meta::pure::functions::collection::tests::contains::comparator(a:ClassWithoutEquality[1], b:ClassWithoutEquality[1]):Boolean[1]
{ $a.name == $b.name }
```

The reference-as-value spelling is a **BARE, `::`-UNQUALIFIED, arity/type-MANGLED
id** (contains.pure:79-84, five occurrences):

```
$col->contains($f1Prime, comparator_ClassWithoutEquality_1__ClassWithoutEquality_1__Boolean_1_)
```

It resolves bare because of the wildcard import
`import meta::pure::functions::collection::tests::contains::*;` at contains.pure:17.
The relevant overload is the `<<PCT.function>>` at contains.pure:25-28
(`contains<Z>(collection:Z[*], value:Z[1], comparator:Function<{Z[1],Z[1]->Boolean[1]}>[1])`),
body `$collection->exists(x | $comparator->eval($value, $x))`.

**`testRemoveDuplicatesPrimitiveStandardFunctionExplicit`** —
`.../platform/pure/essential/collection/transformation/removeDuplicates.pure:53-65`.
**It references THREE names, not one, and they are of two different kinds — the
charter treats this row as a single mechanism and that is wrong:**

- `eq_Any_1__Any_1__Boolean_1_` (lines 55, 58, 61) — a **NATIVE**, `meta::pure::functions::boolean::eq`
- `equal_Any_MANY__Any_MANY__Boolean_1_` (lines 56, 59, 62) — a **NATIVE**, `meta::pure::functions::boolean::equal`
- `cmp_Any_1__Any_1__Boolean_1_` (line 64) — a **USER function**, defined at
  removeDuplicates.pure:37-40 (`cmp(a:Any[1],b:Any[1]):Boolean[1] { $a->toString() == $b->toString() }`)

Imports there are only lines 15-16 (`meta::pure::test::pct::*`,
`meta::pure::functions::collection::tests::removeDuplicates::*`) — the `boolean::`
package is **not** wildcard-imported, so the two natives resolve by Pure's own
default/native visibility.

### a.2 How the engine NAMES a ConcreteFunctionDefinition (the mangling, from source)

The mangled id **is the packageable element's own name**.
`legend-pure-core/legend-pure-m3-core/src/main/java/org/finos/legend/pure/m3/compiler/postprocessing/ConcreteFunctionDefinitionNameProcessor.java`:

- **:44-47** — `String signature = getSignatureAndResolveImports(...); parent._childrenRemove(function); function.setName(signature); parent._childrenAdd(function);`
  — **the function is re-registered in its package under the mangled name.**
- **:81-96** — the builder: `functionName + '_'`, then per parameter
  `<paramTypeName><multSig>_`; with no parameters an extra `'_'` is appended
  (**:89-92**); then `<returnTypeName><returnMultSig>`.
- **:110-113** — the type spelling is `rawType.getName()`, i.e. the **SIMPLE
  (unqualified) type name** — hence `ClassWithoutEquality`, not the FQN.
- **:104** — an unresolved type parameter contributes its parameter NAME.
- **:114-118** — a FunctionType parameter mangles as the literal `"FunctionTypeTODO"`.

Multiplicity spelling,
`.../m3/navigation/multiplicity/Multiplicity.java:210-233`: concrete
`lower==upper` → `_N_`; `[*]` → `_MANY_`; `[a..*]` → `_$a_MANY$_`; other ranges →
`_$a_b$_`; non-concrete → `_<param>_`.

The independent second implementation (descriptor → id) is
`.../m3/navigation/function/FunctionDescriptor.java:176-206`, and **:167-170**
proves the id **is** the package path: `getFunctionByDescriptor` =
`processorSupport.package_getByUserPath(functionDescriptorToId(descriptor))`.
Tracing `comparator(ClassWithoutEquality[1], ClassWithoutEquality[1]):Boolean[1]`
through **:180-198** yields exactly
`comparator_ClassWithoutEquality_1__ClassWithoutEquality_1__Boolean_1_` —
byte-identical to contains.pure:79. `validateType` (**:288-315**) forbids package
paths inside type positions, confirming simple names.

The engine's protocol-side mangler agrees:
`legend-engine-language-pure-compiler/.../HelperModelBuilder.java:297-300`
(`getSignature`), **:316-334** (`terseSignatureSuffix`, note the leading `_` when
parameters exist, **:333**), **:334-337** (`getClassSignature` = substring after
the last `::`), **:339-350** (`getMultiplicitySignature`).

### a.3 The parse node and the compile-time coercion (both engines)

**legend-pure (M3 grammar):** a bare qualified name in value position becomes
`InstanceValue{values:[ImportStub(idOrPath)]}` —
`AntlrContextToM3CoreInstance.java:1188-1215` (specifically **:1194-1195** and
**:1209**). Post-processing resolves the stub
(`InstanceValueProcessor.java:80-83`) and stamps the InstanceValue's type from
the resolved value: `InstanceValueProcessor.java:134-146` → `getGenericType`
**:148-185** → `Instance.extractGenericTypeFromInstance` (the CFD's
`classifierGenericType`, i.e. `ConcreteFunctionDefinition<{A[1],A[1]->Boolean[1]}>`),
multiplicity `1` (**:187-194**).

**legend-engine (Domain grammar / protocol):** `DomainParseTreeWalker.java:1605-1610`
— `instance = new PackageableElementPtr(fullPath)`. Compiled at
`ValueSpecificationBuilder.java:187-206`: `this.context.resolvePackageableElement(...)`
then an `InstanceValue` with `_genericType(packageableElement._classifierGenericType())`,
`_multiplicity("one")`, `_values([packageableElement])` (**:196-200**).
Resolution: `CompileContext.java:290-293` → `PureModel.getPackageableElement`
**:818-823** → `getPackageableElement_safe` **:838-895**, whose function arm is
`getGraphFunctions(fullPath)` **:1103-1127** — a **plain index lookup on the
mangled path**, because `PureModel` indexes functions under
`buildPackageString(package, HelperModelBuilder.getSignature(function))`
(**:1214-1218**).

**So: the engine resolves a function reference AT COMPILE TIME to the function
element itself, and the typed node is an `InstanceValue` of type
`ConcreteFunctionDefinition<{sig}>[1]` (a subtype of `Function<{sig}>`). There is
no separate "function-ref" node kind.** No `handleFunctionExpression` is involved
— that symbol does not appear on this path.

### a.4 EXECUTION: what the reference actually does, and what its relational adapters do

**Both tests are EXPECTED FAILURES of every reference RELATIONAL adapter.** All
99 PCT manifests under both trees were scanned. Exactly 13 mention them, and
**every mention carries an `expectedError`**:
`relational-{h2,duckdb,postgres,snowflake,databricks,clickhouse,memsql,oracle,spanner,sqlserver,trino}`,
plus `java` and `deephaven`. They are **absent** from `core-compiled` and
`core-interpreted` (which have no `EssentialFunctions_manifest.json` at all) — so
they pass only in Pure's own interpreter/compiled runtimes.

The recorded errors are decisive:

- `testContainsWithFunction`, relational-h2 / relational-duckdb:
  `"no viable alternative at input '->meta::pure::functions::collection::contains(...ClassWithoutEquality.all()->...toOne(),meta::pure::functions::collection::tests::contains::comparator(a:meta::pure::functions::collection::tests::contains::ClassWithoutEquality[1],'"`
  — the reference's grammar composer printed the fn-ref as a **full function
  DEFINITION** and the result did not re-parse. The producing code is
  `legend-engine-pure-code-compiled-core/.../core/pure/serialization/toPureGrammar.pure:146-189`:
  `printInstanceValue`'s `f:FunctionDefinition<Any>[1]` arm at **:152** dispatches
  to `printFunctionDefinition`, whose non-lambda arm (**:102-108**) emits
  `printFunctionSignature(...) + "\n{\n body \n}"`. A `ConcreteFunctionDefinition`
  IS a `FunctionDefinition<T>` — m3.pure:2355-2377.
- `testRemoveDuplicatesPrimitiveStandardFunctionExplicit`, same manifests:
  `"No SQL translation exists for the PURE function 'removeDuplicates_T_MANY__Function_$0_1$__Function_$0_1$__T_MANY_'."`

**Consequence for design: the reference relational stack has NO answer here.
There is no engine behaviour to copy for the SQL case. legend-lite must
inline/β-reduce at compile time — and it already does.**

### a.5 Reference-side note that matters for Channel A

The reference does **not** always print a fn-ref as a definition. In our own
Channel A run
(`pct/target/surefire-reports/TEST-...Test_LegendLite_EssentialFunctions_PCT.xml`,
2026-08-27 17:43), the serialized text our compiler received was:

- for the natives: `...removeDuplicates(meta::pure::functions::boolean::eq_Any_1__Any_1__Boolean_1_)`
  (FQN'd mangled name — a NativeFunction is not a `FunctionDefinition`, so
  `printInstanceValue:152` does not catch it and it falls to the
  `a:Any[1]|$a->toRepresentation()` arm at **:180**);
- for the USER functions:
  `...removeDuplicates({a: ...Any[1], b: ...Any[1]|$a->toString() == $b->toString()})`
  and `...contains(^...ClassWithoutEquality(name='f1'), {a: ...ClassWithoutEquality[1], b: ...ClassWithoutEquality[1]|$a.name == $b.name})`
  — i.e. the function's **body inlined as a brace-lambda**.

**UNVERIFIED:** *why* the CFD prints as a `LambdaFunction<Any>` rather than taking
toPureGrammar.pure:102's definition arm. `printFunctionDefinition` (**:86-110**),
`printInstanceValue` (**:146-189**) and our adapter's extension
(`pct_adapter.pure:17-28` — only `TDS` and `CapturedInstance` handlers, no CFD arm)
were all read; none explains it. Likeliest candidate is the interpreted runtime's
`LambdaFunctionCoreInstanceWrapper.toLambdaFunction(...)` view (used at
`legend-pure-runtime-java-engine-interpreted/.../natives/essentials/lang/eval/Evaluate.java:85`),
which would make the runtime classifier `LambdaFunction`. Not confirmed. **The
OUTPUT is verified from the run log; the mechanism is not.**

---

## b) OUR SEAM

### b.1 The exact live errors (from the run log, not reconstructed)

`pct/target/surefire-reports/TEST-org.finos.legend.lite.pct.channelb.ChannelBEssentialTest.xml`:

```
[chB] ERROR ...contains::testContainsWithFunction :: ResolutionException:
  'comparator_ClassWithoutEquality_1__ClassWithoutEquality_1__Boolean_1_' is not a known class,
  mapping, runtime, connection, or database — user elements in a query need a fully qualified name
[chB] ERROR ...removeDuplicates::testRemoveDuplicatesPrimitiveStandardFunctionExplicit ::
  ResolutionException: 'cmp_Any_1__Any_1__Boolean_1_' is not a known class, mapping, runtime,
  connection, or database — user elements in a query need a fully qualified name
```

**The `— user elements in a query need a fully qualified name` suffix is emitted
only when `ref.fullPath()` contains no `::` (`Typer.java:2453-2454`). That is the
whole diagnosis in one string: the name reached the Typer UNQUALIFIED.** Note the
second row names `cmp_`, **not** `eq_`/`equal_` — the two natives already
resolve. The two rows share ONE mechanism (a user function outside the core
packages) but the second row's other two references exercise a **different,
already-working** path.

### b.2 The wall, and the machinery that already exists behind it

`core/src/main/java/com/legend/compiler/spec/Typer.java`, method
`classReference(PackageableElementPtr)` **:2359-2455**:

- **:2360-2398** — exact-FQN probes in order: `ctx.findClass`, `ctx.findEnum`,
  `ctx.isDatabase`, `ctx.findMapping`, `ctx.isExecutionContextElement`.
- **:2400-2404** — **`List<TypedFunction> fns = functionCandidates(ref.fullPath());`**
  — the fn-ref-as-value branch already exists.
- **:2411-2428** — mangled-tail arity disambiguation; **:2421-2427** an
  **opaque-`Function<Any>` escape hatch** when the demangled base exists but no
  arity matches.
- **:2430-2448 — THE ETA-EXPANSION.** Builds
  `TypedLambda(["_fr0","_fr1",…], [Typer.emitCall(fn, argRefs, out)], FunctionType)`.
  `Typer.emitCall` (**:1674-1679**) yields `TypedNativeCall` for a native and
  `TypedUserCall` for a user function.
- **:2451-2454 — THE WALL** (`com.legend.error.ResolutionException`).

`Typer.functionCandidates(String)` **:2299-2319** does the demangle-on-miss:
`ctx.findFunction(name)`; on empty, `SignatureMangle.stripTail(name)` and filter
candidates by `parameters().size() == tailArity(name)` **and**
`returnType().typeName().endsWith(tailReturnTypeName(name))`.
(`Type.Primitive.typeName()` returns the SIMPLE name — `Type.java:115-120` — so
`"Boolean".endsWith("Boolean")` holds.)

`SignatureMangle` (`core/src/main/java/com/legend/compiler/spec/SignatureMangle.java`,
79 lines): regex **:28-29**, `stripTail` **:40-43**, `tailArity` **:47-59**,
`tailReturnTypeName` **:66-78**. Used at **exactly three sites in the whole tree**
— Typer.java:2304/2310/2311, Typer.java:2411/2421, MappingNormalizer.java:1061-1068.
**`NameResolver` never demangles. That is the gap.**

### b.3 Why the name is never qualified (the root cause, mechanically)

Parser node: `core/src/main/java/com/legend/parser/SpecParser.java:1150-1222`,
`parseQualifiedNameStart()` — a qualified/simple name not followed by `(` or
`.all` returns `new PackageableElementPtr(fqn, spanOf(...))` at **:1222**. Exactly
mirrors `DomainParseTreeWalker.java:1605-1610`.

`PackageableElementPtr` (`core/src/main/java/com/legend/protocol/spec/PackageableElementPtr.java:29-49`)
is a record with **one** payload field `fullPath` and a hand-written
`equals`/`hashCode` on `fullPath` only. **Unlike `AppliedFunction`, it has NO
`candidateFqns` field.**

Resolution: `core/src/main/java/com/legend/compiler/NameResolver.java`

- **:1500-1503** — the `PackageableElementPtr` arm:
  `String r = resolveName(ptr.fullPath(), scope)`.
- **:514-522** `resolveName` → **:532-593** `resolveNameMulti` — the ONE core
  lookup. Its tiers are exactly:
  1. **:551-558** — wildcard imports: `scope.knownFqns().contains(pkg + "::" + name)`
  2. **:565-570** — own package (null on query entries; `Scope.preludeOf` sets
     `ownPackage = null`, **:1828-1830**)
  3. **:571-591** — the prelude `PRELUDE_TYPES` map (classes/enums only)
  4. **:592** — **fallthrough returns the name UNCHANGED.**

`scope.knownFqns()` for the ChannelB path is
`Pure.nativeClassFqns() ∪ Pure.nativeEnumFqns() ∪ ctx.elementFqns()`
(NameResolver.java:426-433, called from `pct/.../channelb/ChannelB.java:233-236`).
`PureModelContext.elementFqns()`
(`core/src/main/java/com/legend/compiler/element/PureModelContext.java:145-161`)
adds `model.functions().forEach(e -> out.add(e.qualifiedName()))`.

**And `FunctionDefinition.qualifiedName()` is the PLAIN, UNMANGLED FQN** —
`core/src/main/java/com/legend/model/FunctionDefinition.java:48` (`"my::utils::greet"`),
**:66**; overloads share one FQN and are returned as a list
(`ModelContext.java:63-70`, `TypedFunction` javadoc lines 36-39).

**Therefore `knownFqns` contains `…::contains::comparator` but never
`…::contains::comparator_ClassWithoutEquality_1__…_`. Tier 1 misses, tiers 2-3
miss, `resolveNameMulti` returns the bare mangled string, the ptr passes through
unchanged, and the Typer's `functionCandidates("comparator_…")` then does
`ctx.findFunction("comparator")` — a BARE base.**

That bare lookup is `FunctionCompiler.functionsAt(fqn)`
(`core/src/main/java/com/legend/compiler/element/FunctionCompiler.java:34-80`),
whose bare-name arm (**:43-53**) only unions:

- `Pure.nativeFunctionsAt(bare)` — the native bare index
  (`core/src/main/java/com/legend/builtin/Pure.java:1064-1071`), and
- user functions in **the 10 `CORE_FUNCTION_PACKAGES`**
  (`FunctionCompiler.java:119-132`): `…functions::{collection,string,math,date,boolean,lang,multiplicity,asserts}`,
  `meta::pure::tds`, `meta::pure::tds::extensions`.

`eq` and `equal` are in the native index (`Pure.java:1293` `EQ__ANY_1__ANY_1`,
**:1292** `EQUAL__ANY_MANY__ANY_MANY`) → **they resolve and eta-expand today.**
`comparator` lives at `meta::pure::functions::collection::tests::contains` and
`cmp` at `…tests::removeDuplicates` — **neither package is in the list** → zero
candidates → the wall. **This is the complete root cause for both rows.**

### b.4 Proof that everything AFTER resolution already works

Three independent, currently-passing witnesses:

1. **`core/src/test/java/com/legend/integration/ScalarFunctionIntegrationTest.java:449-454`**
   — `|[1, 2, 1, 3, 1, 3, 3, 2]->meta::pure::functions::collection::removeDuplicates(meta::pure::functions::boolean::eq_Any_1__Any_1__Boolean_1_)`.
   This is removeDuplicates.pure:55 with the FQN spelled out. *(Honest caveat: the
   assertion is only `assertNotNull` — it proves the chain compiles and executes,
   not the value.)*
2. **`core/src/test/java/com/legend/integration/TypeInferenceIntegrationTest.java:808-814`**
   — `|meta::pure::functions::math::acos_Number_1__Float_1_->eval(0.5)` → `ACOS(0.5)`.
   FQN'd mangled pointer → eta-lambda → `eval` β-reduction.
3. **`core/src/test/java/com/legend/integration/TypeInferenceIntegrationTest.java:3205-3217`**,
   `testContainsWithFunction` — the *whole* PCT expression with an inline typed
   lambda instead of the fn-ref, over real `^ClassWithoutEquality(...)` instances,
   asserting `true` on DuckDB.

Supporting infrastructure, all present:

- 3-arg comparator `contains` native: **`Pure.java:1221`** `CONTAINS__T_MANY__T_1__FUNCTION_1`.
- Its lowering: **`core/src/main/java/com/legend/lowering/Scalars.java:1970-2006`**
  — the `args.size()==3 && args.get(2) instanceof SqlExpr.Lambda` arm,
  `LIST_FILTER` + `LIST_LENGTH > 0`, needle substituted into comparator param 0.
- `removeDuplicates(col, eql)` native: **`Pure.java:2108`**; lowering
  **`Scalars.java:1469-1505`**.
- **`Scalars.isEqualityComparator` (`:3039-3056`) recognises EXACTLY the
  eta-expansion shape**: a 2-param `TypedLambda` whose single body statement is a
  `TypedNativeCall` to `…boolean::eq` or `…boolean::equal` over bare parameter
  references. So `eq_`/`equal_` eta-lambdas route to `ListEncodings.orderedDedup`
  (`ListEncodings.java:219-231`, first-occurrence, `LIST_POSITION == index`) —
  which is what removeDuplicates.pure:55-62 asserts.
- `Function<{Any[*],Any[*]->Boolean[1]}>` in a `Function<{T[1],T[1]->Boolean[1]}>[1]`
  slot: explicitly admitted — `core/src/main/java/com/legend/compiler/spec/InferenceKernel.java:223-233`,
  comment *"still admits the equal(Any[*],Any[*]) comparator doctrine, [1] ⊆ [*]"*.
- G½: `UserCallInliner.rewriteSwitch`
  (`core/src/main/java/com/legend/compiler/spec/UserCallInliner.java:335-517`) —
  `case TypedLambda l -> lambda(l, env)` (**:418**) recurses into the eta-lambda
  body; `case TypedUserCall uc -> inlineCall(uc, env)` (**:337**) then splices
  `comparator`'s body with `{a↦_fr0, b↦_fr1}`. The `TypedEval` arm (**:339-347**)
  β-reduces `$f->eval(...)` when `fn instanceof TypedLambda`.

### b.5 Channel A (the charter's follow-on): nothing to do, and here is why

**`pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_EssentialFunctions_PCT.java`
does NOT list either test in `expectedFailures`, and the run reports
`Tests run: 327, Failures: 0` — Channel A passes both rows today.** Confirmed
against the serialized text in the run XML (§a.5): the reference composer hands
us an inline brace-lambda, never a reference.

The `collectRoots` gap the charter asked about is **real as a fact but harmless as
a consequence**:

- `pct/src/main/resources/core_legend_lite_pct/pct_adapter.pure`, **`collectRoots`
  at :173-204**. Exactly four call sites in the tree: **:170** (inside
  `lambdaRoots`), **:189** (the FunctionExpression arm's recursion into
  `parametersValues`), **:193** (the InstanceValue arm's `vs:ValueSpecification`
  case) and **:422** (the adapter entry).
- The **CALL** path ships the callee: **:184-187**,
  `if($callee->instanceOf(ConcreteFunctionDefinition) && $callee->elementToPath()->contains('::tests::'), | $callee->elementToPath(), | [])`.
- The **FN-REF (non-call)** path does **NOT** ship the referenced function's own
  path. A `ConcreteFunctionDefinition` sitting in `$iv.values` matches
  `fd:FunctionDefinition<Any>[1]` (**:194**; m3.pure:2360-2367 confirms CFD ⊂
  FunctionDefinition) → `$fd->lambdaRoots()` (**:161-171**), which emits **only**
  parameter-type roots (**:164-168**) and the body's own roots (**:169-170**) —
  never `$fd->elementToPath()`.
- Consumer: `pct/src/test/java/org/finos/legend/lite/pct/extension/ModelPacker.java`,
  `injectionFromRoots` **:196-233** — a root is sliced as a function **only** if
  `fqn.contains("::tests::")` (**:222-224**). Fed from `PctExecuteNative.java:103-109`.

Because the composer inlines the body, the roots `lambdaRoots` collects (the
`ClassWithoutEquality` class, via the `ve:VariableExpression[1] | $ve.genericType->typeRoots()`
arm at **:201** and the param-type walk at **:164-168**) are exactly the roots the
inlined lambda needs. **The two channels' shapes diverge here: Channel A never
sees a fn-ref; Channel B does. Fixing Channel B buys no Channel A row — contra
`docs/CHANNELB_BURNDOWN_HANDOFF.md:8-11`.**

### b.6 Where the two rows sit in the pins

Both are in the pinned oracle exclusion snapshot
`pct/src/test/resources/oracle/EssentialFunctions_manifest.duckdb.json:31` and
**:103**, so `ChannelBEssentialTest.java:127-144` classifies them
**ENGINE-FRONTIER**, not TRUE-WIRE-BUG — `assertTrue(trueWireBug == 0)`
(**:237-238**) is not at risk. They currently count as WIRE-BUG in the A/B diff.
Fixing both moves: `pass` 305→307 (**:78**), `agreePass` 293→295 (**:164**),
`wireBug` 9→7 (**:165**). All are `>=`/`<=` pins, so nothing breaks — but the
ratchet discipline (**:18-21**, "PASS may only GROW") says the burn must **raise** them.

---

## c) MINIMUM DESIGN — the decisions

**Tenet clearance first.** Per `TENET_CHARTER.md:33-36` (C1.6): *"could this run
with no database attached and no data loaded?"* — resolving a name to a function
and β-inlining its body both pass. **The Java-orchestrates/database-executes
tenet imposes no constraint on this leg.** Likewise "dialect idioms only in
renderers": the existing `contains`/`removeDuplicates` rules are already in
`Scalars`/`ListEncodings` and need no change.

**The binding constraints are three:** `AGENTS.md:123` (NameResolver may not
consult the compiled model or type-check), Invariant 1 (`AGENTS.md:142-158` —
every overload MUST resolve to a concrete signature), and C2.4
(`TENET_CHARTER.md:52-55` — absence is a loud wall, never a plausible value).

**DECISION 1 — the fix is a NAME-QUALIFICATION fix, not a
function-value-semantics feature.** Do not build a new node kind, a new typed
variant, or a new inliner path. The eta-expansion (Typer.java:2430-2448) and G½
already carry the semantics and are pinned green for FQN spellings (§b.4). **The
charter's §3 hypothesis is stale; delete it from the plan.**

**DECISION 2 — the qualification belongs in Phase D (`NameResolver`), and it does
not violate `AGENTS.md:123`.** What must happen is: *given a bare name with a
mangled signature tail, find the wildcard package `P` such that `P::<base>` is a
known PARSED element, and produce `P::<full mangled name>`.* That is string work
over `Set<String> knownFqns` — the parsed element universe. No compiled model. No
types. It lands on the legal side of the row. The **arity and return-type
validation stays in the Typer**, where `functionCandidates` (Typer.java:2304-2316)
already does it against `TypedFunction` signatures — a fact not available in a
`Set<String>` anyway. That split is what makes the design charter-clean rather
than a workaround.

**DECISION 3 — pick ONE of two placements; both legal, differing in blast radius.**

- **(3a) Demangle-on-miss inside `resolveNameMulti`** (NameResolver.java:532-593),
  as a tier that runs **only after** tiers 1-3 have all missed, so an element
  genuinely named `X_Y_1_` still wins on the plain path. Smallest diff; keeps
  `elementFqns()` untouched. Cost: `resolveName` (**:514-522**) throws "ambiguous
  reference" when two wildcard packages both define `<base>` — a loud wall,
  acceptable, but it forecloses cross-package arity disambiguation.
  (`AppliedFunction`'s multi-candidate channel at **:1509-1543** exists precisely
  because call position needed that; for parity, `PackageableElementPtr` would
  need a `candidateFqns` field and `Typer.classReference` would union over it as
  `functionCandidates(AppliedFunction)` does at Typer.java:2328-2351. Defer until a
  witness demands it.)
- **(3b) Emit the engine's own naming: add the mangled FQN alongside the plain one
  in `PureModelContext.elementFqns()`** (**:145-161**), reproducing
  `ConcreteFunctionDefinitionNameProcessor.java:81-96` exactly. This is *conform by
  emission* in its purest form — publishing the element name the reference
  actually uses (**:44-47**) — and needs **zero** change to resolution logic.
  Cost: `elementFqns()` has three other consumers with real blast radius —
  `ScanRelations.java:1962-1981` tail-matches over it, `StoreResolver.java:469-473`
  feeds it to `GenericTypeReflection` (**:82-105**) which re-mangles class FQNs into
  witness prefixes — and mangled function names contain `__`, the same separator
  the witness encoding uses. **If you take 3b, first prove no mangled function name
  can collide with a witness prefix.**

  **Recommendation: 3a**, on blast-radius grounds. Record 3b in the design note as
  the more faithful option deferred for cause.

**DECISION 4 — the wall must get NARROWER-ACCEPTING, never SOFTER.** The accepted
set widens by exactly one predicate: *the demangled base is a known parsed FQN
under an in-scope wildcard*. If no wildcard yields `P::<base>`, `resolveNameMulti`
must still return the name unchanged (**:592**) and `Typer.classReference` must
still throw at **:2451**. Nothing about the refusal changes. (C2.4;
`AGENTS.md:330-332` common mistake #10.)

**DECISION 5 — close the escape hatch that this fix newly exposes.**
Typer.java:2419-2427 returns an **opaque `Function<Any>[1]` `TypedPackageableRef`**
whenever the demangled base exists but no overload matches the tail's arity. Today
that branch is unreachable for a BARE name. After Decision 2 it becomes reachable,
and it turns *"`foo_Bar_9_` names an arity we do not have"* into a silently-typed
opaque value that walls later, elsewhere, with a worse message. **Either delete
that branch for the newly-qualified case or make it throw.** This is the single
most likely way to convert a correct fix into a C2.4 violation.

**DECISION 6 — do not touch `Lowerer.java`, and do not touch the PCT.function
suppression.** Neither is needed. (See TRAPS #7 for why the second was a live
suspicion and why it turned out to be a false alarm.)

**Files touched:** `NameResolver.java` (1846 lines, 1654 of headroom) and possibly
`Typer.java` (3194, 306 of headroom) for Decision 5. **`Lowerer.java` is not on the
path.** No guard file, pin, or allowlist moves except the ratchets in
`ChannelBEssentialTest.java:78/164/165`, which move in the shrink-only direction
the file demands.

---

## d) TRAPS

1. **`Lowerer.java` is exactly 3500 and `FILE_LIMIT` is exactly 3500.**
   `core/src/test/java/com/legend/CodeShapeGuardrailTest.java:35`; the
   `FILE_ALLOWLIST` (**:41-46**) contains **only** `MappingNormalizer.java → 3510`.
   `Lowerer.java` is *not* allowlisted. **One added line turns
   `CodeShapeGuardrailTest` red.** This leg has no reason to go near it.

2. **`UserCallInliner.sameRefs` is IDENTITY, not record equality — and that is the
   safe direction.** `UserCallInliner.java:594-607` compares with `!=`. Two
   *structurally equal but distinct* nodes force a rebuild (harmless); it can never
   merge two distinct nodes. The genuine record-equality hazard would be a
   `TypedSpec`-KEYED hash structure, because `TypedLambda` is a record
   (`.../typed/TypedLambda.java:17`) and two eta-expansions of the same function are
   **structurally identical** (same `_fr0/_fr1` names, same callee, same `ExprType`).
   **Every `TypedSpec`-keyed map/set in `core/src/main/java` was enumerated — there
   are 9, and all 9 are identity-based:** `SyntheticHeads.java:1031-1032` and
   `:1219-1220`, `DateSplit.java:68-71`, `Anchors.java:39-40` and `:63-64`,
   `InnerDemand.java:377` and `:626-627`, `StoreResolver.java:1946-1947`,
   `Lowerer.java:744-745`, plus `SpecCompiler.java:44`. Every
   `Map<String, TypedSpec>` has `TypedSpec` in **value** position. On the MIR side
   there are exactly two `SqlExpr`-keyed structures (`Lowerer.java:1510-1511`,
   `WhereMerge.java:35`), both `IdentityHashMap`. **Conclusion: no dedup/merge
   hazard exists today. But `TypedNativeCall` carries a hand-written structural
   `equals` (`TypedNativeCall.java:39-50`) that recurses into `TypedFunction` →
   `Function` → the whole parsed AST; if anyone ever hashes typed nodes, duplicated
   eta-lambdas will collide by value. Do not introduce one.**

3. **Two live structural-equality RECOGNISERS do fire on eta-lambdas — check both.**
   - `Scalars.isEqualityComparator` (`Scalars.java:3039-3056`) matches the eta shape
     for `eq`/`equal`. This is why removeDuplicates.pure:55-62 already works — but it
     is a *pattern match on our own synthesis*, so any change to the eta-expansion's
     shape (renaming `_fr` params, wrapping the body) **silently changes the SQL from
     `orderedDedup` to the `Dedup.keptDedup` fold**. Same result, different plan; the
     coupling is undocumented.
   - `Comparators.direction(TypedSpec)` (`core/src/main/java/com/legend/lowering/Comparators.java:102-121`)
     recognises `{x,y|$x->compare($y)}` structurally. An eta-expansion of `compare_…`
     produces exactly that shape — relevant to **Leg 5**, and a reason to coordinate.
     `Comparators.select` (**:36-95**) uses `SqlExpr.equals` structurally at
     **:50/:53/:54** after substitution; `LambdaBinding.java:63-70` documents that
     stamping comparator params previously *broke* that recogniser's structural
     equality (gate-caught on `chB-std testMax/testMin`).

4. **The `_fr0`/`_fr1` binder names are FIXED, not fresh** (Typer.java:2437).
   `UserCallInliner.reserveFreshNames`/`bumpPast` (**:135-155**) only guards the
   `_i<N>` namespace. Inside an inlined body `lambda(l, env)` α-renames to `_i<N>`
   (**:544-563**), so the compiler side is safe. **The exposure is at the SQL
   level:** `Scalars.java:2005` builds
   `new SqlExpr.Lambda(List.of(comp.params().get(1)), body)` — the SQL lambda
   parameter *is* `_fr1`. Two **nested** comparator-carrying calls whose comparators
   are both eta-lambdas would produce nested SQL lambdas binding the same name.
   `Scalars.java:1490-1495` already guards the dedup *accumulator* names by nesting
   depth, but **not** the comparator parameter. Neither failing row nests, so this is
   latent — but it is the exact bug class the audit note at **:1490-1494** was written
   about.

5. **The escape hatch at Typer.java:2419-2427 is the NO-FALLBACK risk this leg
   introduces.** See Decision 5. Currently dead for bare names; live the moment they
   qualify.

6. **Stale charter assumptions to strike from the plan.**
   - §3's *"Our parser/resolver needs function-reference-as-value"* — **built and
     pinned** (Typer.java:2400-2448; three passing witnesses in §b.4).
   - §3's *"check the FN-REF (non-call) path ships too for Channel A"* — **Channel A
     passes both rows today** (327 run, 0 failures, neither in `expectedFailures`).
     The `collectRoots` fn-ref gap is real (pct_adapter.pure:194 vs :184-187) but
     unreachable for this shape.
   - `CHANNELB_BURNDOWN_HANDOFF.md:8-11` (*"Burning Channel B burns the matching
     Channel A rows for free"*) — **does not apply to Leg 4**; there is no matching
     Channel A row.
   - The handoff's one-line diagnosis (*"a user function passed BY REFERENCE resolves
     as an element ref"*) is half right: it **is** an element ref, and that is correct
     and engine-faithful — the defect is that the element ref is never *qualified*.
   - `docs/SIMPLE_NAME_AUDIT.md` carries a `⚠ SUPERSEDED — 2026-08-06` banner and
     targets the deleted `engine/com.gs.legend` module. **Do not act on it.** Likewise
     `progress/baseline-failures.txt:38` names the deleted module.
   - `docs/NAME_RESOLUTION_BUG.md:1-45` is live and directly on point: the bare-name
     *scan* fallbacks in `findDatabase`/`findClass`/`findJoin` were **deliberately
     deleted** (task #110, 2026-08-02) and those lookups are now exact-FQN-only.
     **Your fix must not reintroduce a scan.** Qualifying through the file's own
     wildcard imports is the sanctioned mechanism; tail-matching over the model is
     the deleted one.
   - `docs/FQN_MIGRATION.md` (RESOLUTION 2026-07-09) declares step 2 — "NameResolver
     rewrites bare function names to single FQNs" — **CLOSED BY DESIGN**, because
     multi-home simple names are *different functions* and the resolver would have to
     guess before types are known. **This does not forbid Decision 2, and the
     distinction is load-bearing: a MANGLED id names exactly ONE overload by
     construction (ConcreteFunctionDefinitionNameProcessor.java:53-77 rejects a
     duplicate mangled name in a package), so there is nothing to guess.** State that
     reconciliation explicitly in the commit message, or a future reader will read
     this fix as a reversal.

7. **A false alarm, recorded so nobody re-chases it.**
   `FunctionCompiler.addModelOverloads` (**:92-115**) drops `<<PCT.function>>`
   definitions whenever *any* native key at the same FQN exists — the test at
   **:105-106** is `nativeKeysAt(bare).anyMatch(k -> k.startsWith(fqn + "("))`, which
   is **arity-blind**. Confirmed empirically that it fires for `contains`:
   `[legend-lite] PCT.function 'meta::pure::functions::collection::contains' suppressed (native is the definition)`
   appears in `pct/target/surefire-reports/TEST-…ChannelBRelationTest.xml`. So the
   3-arg comparator overload at contains.pure:25 **is** dropped from the model.
   **Harmless only because `Pure.java:1221` registers the 3-arg comparator native
   itself and `Scalars.java:1970-2006` lowers it.** Do not "fix" the suppression as
   part of this leg: making it signature-precise would un-suppress dozens of
   PCT.function overloads across the platform tree (the run logs show 78 suppressed
   FQNs across five suites) — a full-lane-measurement change, not a two-row fix.

8. **Verification must check the VALUE, not just the absence of a wall.**
   `ScalarFunctionIntegrationTest.java:449-454` asserts only `assertNotNull` — do not
   read it as a value pin. The real check is the PCT asserts themselves, which are
   order-sensitive with **no `->sort()`** at removeDuplicates.pure:55-64.

---

## e) CONFIDENCE + LIVE PROBES

**HIGH confidence (read from source and corroborated by run artefacts):**

- The mangled-name grammar and that it *is* the element name
  (ConcreteFunctionDefinitionNameProcessor.java:44-47/81-96;
  FunctionDescriptor.java:176-206; Multiplicity.java:210-233), traced by hand to
  byte-identical agreement with contains.pure:79 and removeDuplicates.pure:64.
- The engine's compile-time coercion and the typed node it produces
  (ValueSpecificationBuilder.java:187-206; InstanceValueProcessor.java:134-194).
- That the reference relational stack **cannot** do this — all 13 mentioning
  manifests are exclusions, with the two distinct error texts quoted.
- The exact failure mechanism in our tree, end to end (§b.3), corroborated by the
  `— user elements in a query need a fully qualified name` suffix in the live error.
- That `eq_`/`equal_` already work and only `cmp_`/`comparator_` fail.
- That the eta-expansion, G½ inlining, the 3-arg `contains` native + lowering, and
  `removeDuplicates` first-occurrence dedup all already exist and are exercised.
- The exhaustive negative result on record-equality hazards.
- Channel A passes both rows; the serialized text it receives is the inlined
  brace-lambda.
- `Lowerer.java` = 3500 = `FILE_LIMIT`, not allowlisted.

**MEDIUM confidence:**

- **That the two rows go fully GREEN once the name qualifies.** Evidence is strong
  but indirect. For `testContainsWithFunction` the inline-lambda equivalent passes
  end-to-end (TypeInferenceIntegrationTest.java:3208-3217). For the removeDuplicates
  row, `testRemoveDuplicatesPrimitiveNonStandardFunction` **PASSES** with the *same*
  mixed collection `[1, 2, '1', '3', 1, 3, '3', 2]`, the *same* toString comparator
  semantics as `cmp`, and *no* `->sort()` — the closest possible analogue to line 64.
  Neither row was executed.
- The residual risk on removeDuplicates.pure:61-62 (`[1, 2, '1', '3', 3]`, unsorted,
  mixed types, `eq_` comparator). Note
  `testRemoveDuplicatesPrimitiveStandardFunctionMixedTypes` currently FAILS with
  `expected [1,2,3,'1','3'] / actual ['1','3',1,2,3]` — but that is a **sort-order**
  defect (that test calls `->sort()`), not a dedup defect: the element *set* is right.
  **Leg 5 owns it.** Lines 61-62 do not sort, so they should be unaffected.

**UNVERIFIED — stated as such:**

- Why the reference prints a `ConcreteFunctionDefinition` as a brace-lambda rather
  than taking toPureGrammar.pure:102's definition arm. Best hypothesis: the
  interpreted runtime's `LambdaFunctionCoreInstanceWrapper.toLambdaFunction`
  view. **What would settle it:** print `$v->type()->elementToPath()` inside the
  `printInstanceValue` dispatch during one Channel A run. Not needed for the Channel
  B fix; needed before anyone *relies* on Channel A's inlining.
- Whether the reference relational adapters' *only* obstacle for
  `testContainsWithFunction` is the composer round-trip, or whether a second wall
  sits behind it.
- Whether a mangled function FQN can collide with a `GenericTypeReflection` witness
  prefix (both use `__`). Only matters under Decision 3b.

### Live probes, in order

1. **The one-line falsifier, before any code changes.** Compile the *fully
   qualified* spellings through the existing pipeline and confirm both pass:
   `|['1', 2, '1', '3', 1, 3, '3', 2]->meta::pure::functions::collection::removeDuplicates(meta::pure::functions::collection::tests::removeDuplicates::cmp_Any_1__Any_1__Boolean_1_)`
   (expect `['1', 2, '3']`), and the `contains` equivalent with
   `meta::pure::functions::collection::tests::contains::comparator_ClassWithoutEquality_1__ClassWithoutEquality_1__Boolean_1_`
   (expect `true`). **If these two pass, the whole leg is confirmed to be a
   name-qualification fix and nothing else. If either fails, the diagnosis is
   incomplete and the plan must be re-derived before writing code.**
2. **Then the scoped Channel B run:** `cd pct && mvn -o test -Dtest=ChannelBEssentialTest -Dchb.only=contains`
   and `-Dchb.only=removeDuplicates`. Every currently-passing sibling must stay green
   — `testContainsPrimitive`, `testContainsNonPrimitive`,
   `testRemoveDuplicatesPrimitiveStandardFunctionSimple`, `…NonStandardFunction`,
   `…EmptyList` are all PASS today.
3. **A NEGATIVE probe for Decisions 4/5** — the one that proves you widened the
   accepted set rather than softening the refusal. Two cases: (a) a bare mangled id
   whose base does not exist anywhere must still raise the wall; (b) a bare mangled
   id whose base **does** exist under an in-scope wildcard but with a **different
   arity** (e.g. `comparator_Integer_1__Boolean_1_`) must **also** wall — not return
   the opaque `Function<Any>` from Typer.java:2421-2427. Case (b) is the regression
   this fix can introduce; pin it.
4. **The full lane**, then ratchet `ChannelBEssentialTest.java:78` (305→307),
   **:164** (293→295) and **:165** (9→7) with a dated justification.
5. **Leave `Test_LegendLite_EssentialFunctions_PCT.java` alone** — Channel A is green
   on both rows and its exclusion list does not mention them.

---

## OPEN QUESTIONS

1. **Decision 3a or 3b?** Demangle-on-miss in `resolveNameMulti` (small diff,
   ambiguity wall on multi-package bases), or emit the engine's mangled element names
   into `PureModelContext.elementFqns()` (zero resolution-logic change, more faithful
   to `ConcreteFunctionDefinitionNameProcessor.java:44-47`, but three other consumers
   — `ScanRelations.java:1970`, `StoreResolver.java:473`,
   `GenericTypeReflection.java:82-105` — must first be cleared of `__` collisions)?
   Recommend 3a; the choice is the burn's and should be recorded, not defaulted.
2. **Should `PackageableElementPtr` gain a `candidateFqns` field, mirroring
   `AppliedFunction`?** Neither failing row needs it (each base is unique under one
   wildcard). It becomes necessary the first time two in-scope packages define the
   same base name with different arities. Build now for symmetry, or defer?
3. **How does `docs/FQN_MIGRATION.md`'s "step 2 CLOSED BY DESIGN" ruling get
   amended?** It is right for *un*mangled call-position names and wrong-by-omission
   for *mangled* value-position ids. Leaving it unamended guarantees a future reader
   treats this fix as a reversal.
4. **Does `Typer.java:2419-2427`'s opaque-`Function<Any>` escape have a live consumer
   today?** Its comment (**:2405-2410**, ledger cluster 26) cites "a mangled id naming
   a function this platform spells differently, e.g. the TDS groupBy the checker
   desugars at call sites". If that consumer is real, Decision 5 cannot simply delete
   the branch and must gate it instead. The witness was not found.
5. **Is `Comparators.direction` the intended route for Leg 5's
   `testSimpleSortWithFunctionVariables`?** An eta-expansion of `compare_…` produces
   exactly its recognised shape. Legs 4 and 5 may share a mechanism; whoever burns
   second should check before duplicating.
6. **Does the reference-side CFD→brace-lambda printing (§a.5) hold for every shape,
   or only these two?** If it is an accident of the interpreted runtime rather than a
   contract, Channel A's green on these rows is fragile in a way nobody has recorded.
