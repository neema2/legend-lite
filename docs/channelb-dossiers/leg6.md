# Leg 6 — `toString` over class instances (6a) + the `relation::toString` overload tie (6b)

**Rows:** `testPersonToString`, `testComplexClassToString` (6a);
`testRemoveDuplicatesEmptyListExplicit` (6b).
**Charted errors:** `toString over ClassType[...] is not modeled`;
`ambiguous overload of 'relation::toString': 2 candidates tie`.

> ## THREE FINDINGS THAT CHANGE THE CHARTER
>
> **F-1 (6a). The reference has NO generic instance printer.** There is no
> `ClassName(prop=val, …)` form anywhere in legend-pure or legend-engine.
> `toString` over a non-primitive is exactly two rules: *if the classifier (or a
> generalization) declares a zero-arg `toString()` qualified property, EXECUTE
> it; otherwise return the M4 instance's NAME* (`ToString.java:52-69`).
> `testPersonToString` takes the second branch; `testComplexClassToString` takes
> the **first**. They are not two instances of one format — they are the two arms
> of a dispatch, and only 6a-Person involves an id.
>
> **F-2 (6a). `testComplexClassToString` is not a print-form problem at all.**
> `ClassWithComplexToString` **declares its own `toString()`**
> (`toString.pure:181-188`). The expected string is the output of that user body
> — `format(...)` over enum comparisons. Nothing about instance rendering is
> required. The charter's "ONE owner beside LiteralSpelling/printForm" does not
> apply to this row.
>
> **F-3 (6b). The reference ALSO ties on scoring — it simply never has both
> candidates.** `TypeMatch.newTypeMatch` collapses *every* match against a
> bottom-typed value to the singleton `BOTTOM_TYPE_MATCH`
> (`TypeMatch.java:78-101, 394-397`), and `GenericTypeMatch` short-circuits before
> type-argument comparison for a bottom value (`GenericTypeMatch.java:236-239`).
> `Any[1]` and `Relation<T>[1]` against `Nil[1]` produce **`.equals()`-identical**
> `FunctionMatch` objects. The reference is saved by its **candidate set**, not
> its scoring: `relation::toString` lives in `core_functions_relation`, which
> *depends on* `platform` — so it does not exist when `removeDuplicates.pure` (a
> `platform` source) is compiled. **A "fix the scoring to the engine's real rule"
> mandate has no target: the engine's real rule ties.** The honest fix is a
> candidate-set/provenance fix.

See `README.md` for the shared tenet quick-reference and provenance notes.

---

## a) REFERENCE SEMANTICS

### 6a — the two arms

**The native declaration** —
`legend-pure/legend-pure-core/legend-pure-m3-core/src/main/resources/platform/pure/essential/string/toString/toString.pure:19-24`:

```
native function <<PCT.function>> { doc.doc='' }
    meta::pure::functions::string::toString(any:Any[1]):String[1];
```

**The interpreted implementation (the normative rule)** —
`legend-pure/legend-pure-runtime/legend-pure-runtime-java-engine-interpreted/src/main/java/org/finos/legend/pure/runtime/java/interpreted/natives/essentials/string/toString/ToString.java:52-69`:

```java
CoreInstance value = Instance.getValueForMetaPropertyToOneResolved(params.get(0), M3Properties.values, processorSupport);
CoreInstance type  = processorSupport.getClassifier(value);
if (Type.isPrimitiveType(type, processorSupport)) {
    return ValueSpecificationBootstrap.newStringLiteral(this.repository, value.getName(), processorSupport);
} else {
    CoreInstance toStringFunc = findBestToStringFunction(type, processorSupport);
    if (toStringFunc == null) {
        return ValueSpecificationBootstrap.newStringLiteral(this.repository, value.getName(), processorSupport);
    } else {
        return this.functionExecution.executeLambdaFromNative(toStringFunc, params, ...);
    }
}
```

`findBestToStringFunction` (**:83-86**) is
`_Class.findQualifiedPropertyWithNoExplicitArgsUsingGeneralization(type, "toString", …)`,
defined at
`legend-pure-core/legend-pure-m3-core/src/main/java/org/finos/legend/pure/m3/navigation/_class/_Class.java:114-120`:
walk `Type.getGeneralizationResolutionOrder(classifier)`, flat-collect qualified
properties, `detect` the first whose name is `"toString"` **and whose function
type has exactly 1 parameter** (i.e. only `$this`). Its own javadoc
(`_Class.java:72-79`) says: "Returns the most specific toString function. If there
is more than one most specific function, then an **arbitrary** one is returned."

**The compiled implementation (agrees)** —
`legend-pure-runtime/legend-pure-runtime-java-engine-compiled/.../support/CompiledSupport.java:1189-1244`.
In order: `null → "NULL"` (**:1191-1194**); Boolean/Number/PureDate/String →
`primitiveToString` (**:1195-1210**); `BaseCoreInstance`/`AbstractLazyCoreInstance`
→ `ModelRepository.possiblyReplaceAnonymousId(getName())` (**:1211-1215**);
otherwise reflectively invoke the generated class's `toString(ExecutionSupport)` —
the compiled form of the qualified property (**:1221-1233**); else
`ConsoleCompiled.getId` (**:1234-1242**).

Primitive spellings, **:1246-1302**: `boolean → "true"/"false"`;
`int/long → Integer/Long.toString`; `float → (double)`;
`double → (value == 0.0d) ? "0.0" : DECIMAL_FORMAT.format(value)`;
`BigDecimal → toPlainString()`; `PureDate → value.toString()`.

**The anonymous-id form** —
`legend-pure-core/legend-pure-m4/src/main/java/org/finos/legend/pure/m4/ModelRepository.java`:

- **:85** `private static final String ANONYMOUS_NAME_PREFIX = "@_";`
- **:86** `ANONYMOUS_PADDING_LENGTH = Integer.toString(Integer.MAX_VALUE, 32).length()` → `"1vvvvvv"` → **7**
- **:902-916** `nextAnonymousInstanceName()` = `"@_"` + base-32 counter
  **left-zero-padded to 7 digits** → `@_0000000`, `@_0000001`, … (total length 9)
- **:923-928** `isAnonymousInstanceName` = non-null, length ≥ 9, `startsWith("@_")`
- **:935-937** `possiblyReplaceAnonymousId(id)` returns the **literal string
  `"Anonymous_StripedId"`** when the id is anonymous.

So the interpreted runtime prints `@_0000123`; the compiled runtime prints the
literal `Anonymous_StripedId`. **That is precisely why the test accepts either
prefix.**

#### `testPersonToString` — EXACT source, `toString.pure:137-141`

```
function <<PCT.test>> meta::pure::functions::string::tests::toString::testPersonToString<Z|y>(f:Function<{Function<{->Z[y]}>[1]->Z[y]}>[1]):Boolean[1]
{
    let str = ^STR_Person(firstName='Pierre', lastName='Doe')->toString();
    assert($f->eval(|$str->startsWith('Anonymous_') || $str->startsWith('@_')));
}
```

**There is NO exact expected string.** The assertion is a two-way prefix test
only: `'Anonymous_'` or `'@_'`. Class under test, `toString.pure:165-169`:
`STR_Person { firstName : String[1]; lastName : String[1]; }` — two properties,
both `String[1]`, **no Float / Date / Decimal, no to-many, no nesting, no nulls,
and no `toString()` qualified property**. Property values are provably irrelevant
to the output.

#### `testComplexClassToString` — EXACT source, `toString.pure:154-157`

```
function <<PCT.test>> meta::pure::functions::string::tests::toString::testComplexClassToString<Z|y>(f:…)…
{
    assertEq('// Warning: Good for gin -- Sad times no tonic', $f->eval(|^ClassWithComplexToString(errorType=ErrorType.NoTonic,errorMessage='Sad times')->toString()));
}
```

**EXACT expected output, character for character:**

```
// Warning: Good for gin -- Sad times no tonic
```

(`//`, space, `Warning:`, space, `Good for gin`, space, **two** hyphens, space,
`Sad times`, space, `no tonic`. No trailing whitespace, no quotes.)

The class, `toString.pure:178-189`:

```
Class meta::pure::functions::string::tests::toString::ClassWithComplexToString {
   errorType : ErrorType[1];
   errorMessage : String[1];
   toString() {format('// Warning: %s -- %s %s', [if($this.errorType->equal(ErrorType.NoGin),
                                              |'No Gin - disaster',
                                              |'Good for gin'),
                                             $this.errorMessage,
                                             if($this.errorType->equal(ErrorType.NoTonic),
                                              |'no tonic',
                                              |'good for tonic')
                                            ])}:String[1];
}
```

`ErrorType` is `Enum { NoGin, NoTonic }` (`toString.pure:160-163`). With
`errorType=NoTonic`, `errorMessage='Sad times'`: arg1 = `'Good for gin'`,
arg2 = `'Sad times'`, arg3 = `'no tonic'`.

**Critical `format` detail (this is what makes the expected string unquoted).**
`%s` uses `pureToString`, `%r` uses `toRepresentation` —
`PureStringFormat.java:53-58` vs **:59-64**. `toRepresentation` **quotes and
escapes** Strings (`toRepresentation.pure:21`:
`'\'' + $s->replace(…) + '\''`), so had the body used `%r` the expected value
would have been `// Warning: 'Good for gin' -- 'Sad times' 'no tonic'`. It uses
`%s`, so the values are bare. (Interpreted `Format.java:64` resolves
`toRepresentation_Any_1__String_1_` for the `%r` slot only.)

**How the platform List/Pair prints are produced** (proving the "qualified
property" rule is the general mechanism, not a special case):
`platform/pure/anonymousCollections.pure:21-24` declares `Pair.toString()` =
`['<', first, ', ', second, '>']->joinStrings('','','')`, and **:36-39** declares
`List.toString()` = `values->map(v|$v->toString())->joinStrings('[', ', ', ']')`.
Those are the sole source of `'<a, b>'` (`testPairToString`, `toString.pure:126-127`)
and `'[a, b, c]'` / `'[[a, b], c]'` (`testListToString`, `toString.pure:120-121`).

**Sibling rows that pin the same rule** (all `value.getName()`, no qualified
property): `testClassToString` expects `'STR_Person'` — the **simple** name
(`toString.pure:145`); `testEnumerationToString` expects
`'STR_GeographicEntityType'` and `'CITY'` (**:150-151**). Contrast
`toRepresentation`, which for a class gives the **full path**
`'meta::pure::functions::string::tests::toRepresentation::ST_Person'`
(`toRepresentation.pure:137`) and for an instance `'<Anonymous_…'` / `'<@_…'` —
angle-bracketed (`toRepresentation.pure:126-133`, format at **:27-28**).

### 6a — OBJECT IDENTITY: yes, and it is bounded

`testPersonToString`'s output **is** an object identity — `@_` + a per-repository
monotonic counter, or the compiled runtime's constant `Anonymous_StripedId`. It is
not reproducible from rows. **But the assertion is only a prefix test**, so a
SQL-executing engine needs a *stable, per-construction-site* string with one of
those two prefixes — not the reference's counter value. Our IR already has exactly
that (§b). The homework doc reached the same conclusion:
`docs/CANONICAL_RENDER_HOMEWORK.md:27` — "`Anonymous_…`/`@_…` id (**NOT canonical
— identity, excluded from byte channel**) or user-defined toString() qualified
property".

### 6a — what the reference's own relational adapters do

**No relational adapter passes either row.** Exhaustive sweep of every
`pct-manifests/*/EssentialFunctions_manifest.json` in legend-engine:

| adapter | `testPersonToString` | `testComplexClassToString` |
|---|---|---|
| relational-duckdb | `"Assert failed"` | `"type not supported: meta::pure::functions::string::tests::toString::ErrorType"` |
| relational-sqlserver | `"Assert failed"` | same |
| relational-trino | `"Assert failed"` | same |
| relational-clickhouse / memsql | `class java.lang.Long cannot be cast to class java.lang.Boolean` | same |
| relational-oracle | `class java.lang.String cannot be cast to class java.lang.Boolean` | same |
| relational-postgres/h2/databricks/spanner/snowflake | *(absent → passes)* | same |
| java (Test_JAVA) | *(absent)* | `expected: '// Warning: Good for gin -- Sad times no tonic'` / `actual: '_pure.internal.…ClassWithComplexToString_Impl…'` |
| deephaven | `function not supported yet: …boolean::or_…` | `function not supported yet: …string::toString_Any_1__String_1_` |

The `"type not supported: …ErrorType"` origin is **not** toString:
`legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-PCT/…/core_external_test_connection/pct_relational.pure:835-836`
— the relational PCT adapter materializes a constructed instance into a real temp
TABLE (one column per `isPrimitiveValueProperty` property + a `pureId__` PK,
**:837-838**), and `pureTypeToDataType` has no relational data type for an
Enumeration, so `->toOne('type not supported: ' + …)` fails.

**Crucially: `core-interpreted/` and `core-compiled/` have NO
`EssentialFunctions_manifest.json` at all** (verified by `find` over both repos —
only ScenarioQuant/Standard/Variant/Unclassified/Relation exist). Zero exclusions
⇒ **all three target tests PASS on the reference interpreted and compiled
platforms.** That, not the relational manifests, is Channel B's bar (§b).

### 6b — reference semantics

**The test**,
`legend-pure/…/platform/pure/essential/collection/transformation/removeDuplicates.pure:81-85`:

```
function <<PCT.test>> meta::pure::functions::collection::tests::removeDuplicates::testRemoveDuplicatesEmptyListExplicit<Z|y>(f:…)…
{
    assertEquals([], $f->eval(|[]->removeDuplicates({x, y | $x->toString() == $y->toString()})));
    assertEquals([], $f->eval(|[]->removeDuplicatesBy({x | $x->toString()})));
}
```

Overloads in that file: 3-arg native
`removeDuplicates<T,V>(col:T[*], key:Function<{T[1]->V[1]}>[0..1], eql:Function<{V[1],V[1]->Boolean[1]}>[0..1])`
(**:24**); 2-arg `removeDuplicates<T>(col:T[*], eql:Function<{T[1],T[1]->Boolean[1]}>[1])`
whose body is `$col->removeDuplicates([], $eql)` (**:26-29**); 1-arg (**:31-34**).
The relational-DuckDB manifest's expected error names
`removeDuplicates_T_MANY__Function_$0_1$__Function_$0_1$__T_MANY_` — the **3-arg
native** — proving the reference resolved the 2-arg overload and inlined its body,
then died at SQL translation
(`legend-engine-xts-relationalStore/…/core_relational/relational/pureToSQLQuery/pureToSQLQuery.pure:2685`).
No ambiguity anywhere.

**The full reference resolution algorithm, exhaustively:**

1. **Candidate set** — `FunctionExpressionMatcher.getFunctionsWithMatchingName`
   (**:153-157**): `processorSupport.function_getFunctionsForName(name)` filtered to
   functions whose `_package()` is in `getValidPackages` (**:159-170**) = the
   expression's import group ∪ `coreImport` ∪ Root (`Imports.java:52-65`).
2. **`coreImport` contains BOTH packages** — `platform/pure/grammar/m3.pure:175-201`:
   `'meta::pure::functions::string'` at **:192** and
   `'meta::pure::functions::relation'` at **:199**. *Import filtering does not
   disambiguate.*
3. **Per-candidate match** — `FunctionMatch.newFunctionMatch` (**:114-156**): name
   equal, **arity equal**, then per-parameter `GenericTypeMatch` (covariant,
   `targetParameterMatchBehavior=MATCH_ANYTHING`,
   `valueParameterMatchBehavior=MATCH_CAUTIOUSLY`) and `MultiplicityMatch`; any
   `null` ⇒ candidate rejected.
4. **Ordering** — `FunctionMatch.compareTo` (**:68-103**): arity, then **all type
   matches in order**, then **all multiplicity matches in order**. That is the
   *complete* criterion list — no arity-of-generics, no native-vs-module, no package
   preference, no return-type tiebreak.
5. **Type-match lattice** — `TypeMatch`: `SimpleTypeMatch(typeDistance)` where
   distance is `Type.getGeneralizationResolutionOrder(sub).indexOf(super)`
   (**:418-433**, exact = 0); `BOTTOM_TYPE_MATCH` when
   `Type.isBottomType(subType)` (**:78-101, 394-397**); `NON_CONCRETE_MATCH`;
   `NULL_MATCH`; plus structural `RelationTypeMatch`/`FunctionTypeMatch`. Ordering:
   `SimpleTypeMatch < NON_CONCRETE < …`, and `BOTTOM_TYPE_MATCH.compareTo` returns
   **0 against itself, `-1` vs NULL, `+1` vs everything else** — i.e. **all bottom
   matches are one indistinguishable value**.
6. **Bottom short-circuit** — `GenericTypeMatch.java:236-239`:
   `if (Type.isBottomType(covariant ? valueRawType : targetRawType) || Type.isTopType(...)) return new GenericTypeMatch(rawTypeMatch);`
   — returns with **empty** `typeArgumentMatches` and `multiplicityArgumentMatches`,
   so `Relation<T>`'s type argument is never examined.
7. **Tie handling** — `getBestFunctionMatch` (`FunctionExpressionMatcher.java:90-151`):
   `bestFunctions.size() > 1` ⇒ `throw new PureCompilationException("Too many matches for …")`
   (**:140-148**). `FunctionExpressionProcessor` uses
   `findMatchingFunctionsInTheRepository` for the candidate list (**:387**) and then
   calls `getBestFunctionMatch(..., lenient=false)` at **:210** to confirm.

**Applying it to `$x->toString()` with `$x : Nil[1]`.** `Nil` is Pure's bottom type
(`Type.java:123-126`, `isBottomType`). `Nil` is not a `RelationType`
(`_RelationType.java:55-63`), so `TypeMatch.newTypeMatch(Any, Nil)` and
`TypeMatch.newTypeMatch(Relation, Nil)` both fall to **:394-397** → the same
`BOTTOM_TYPE_MATCH` singleton. Both then take `GenericTypeMatch.java:236-239`. Both
multiplicities are `[1]`. ⇒ **`FunctionMatch.equals` is true; `compareTo` is 0.**
If both were visible, `getBestFunctionMatch` would raise
`Too many matches for toString(Nil[1])`.

**Why the reference never gets there — repository layering.** `relation::toString`
is defined at
`legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-functions-relation/legend-engine-pure-functions-relation-pure/src/main/resources/core_functions_relation/relation/functions/toString.pure:19`
and **:24** (2 overloads, 1-arg and 2-arg). That repo's descriptor,
`core_functions_relation.definition.json`, reads:

```json
{ "name": "core_functions_relation",
  "pattern": "(meta::pure::functions::relation)(::.*)?",
  "dependencies": ["platform", "platform_dsl_tds", "platform_precise_primitives",
                   "core_functions_unclassified", "core_functions_variant"] }
```

`removeDuplicates.pure` is a **`platform`** source. `platform` is compiled before —
and cannot see — `core_functions_relation`. **The only `toString` in the whole
legend-pure platform is the single native at `toString.pure:24`** (verified by
exhaustive grep of `platform/**`, which returned exactly one function declaration
and three qualified-property declarations: `anonymousCollections.pure:21`, **:36**,
and `toString.pure:181`). Candidate count at that call site: **1**. No tie possible.

---

## b) OUR SEAM

**Which harness sets the bar.** `pct/src/test/java/org/finos/legend/lite/pct/channelb/ChannelB.java:24-39`
— "**OUR parser compiles the `PCT.test` functions, OUR platform executes them** …
The adapter is the IDENTITY: `$f->eval(|expr)` β-reduces to `expr` … the whole test
body then compiles and runs as ordinary pure — asserts included." `eliminateAdapter`
(**:271-305**) rewrites the body; `runOneInner` (**:216-249**) compiles and executes
it against a fresh in-memory DuckDB. `ChannelBEssentialTest.java:32-37` scopes the
model to `legend-pure/…/platform/pure` with discovery under `essential/`.

**Consequence:** unlike every reference relational adapter, Channel B *does*
execute `let str = ^STR_Person(...)->toString();` on our platform. The reference
relational manifests are therefore **not** the target; the interpreted/compiled
platform semantics are (and those pass all three — §a).

Current Channel-A ledger:
`pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_EssentialFunctions_PCT.java:65`
pins `testComplexClassToString` with **our** message,
`"toString over ClassType[fqn=meta::pure::functions::string::tests::toString::ClassWithComplexToString] is not modeled"`
(comment at **:60-64** calls it "a designed refusal"). `testPersonToString` and
`testRemoveDuplicatesEmptyListExplicit` are **not** in that list. Our frontier
oracle `pct/src/test/resources/oracle/EssentialFunctions_manifest.duckdb.json`
carries the reference's rows at **:97**, **:418**, **:442**.

### 6a — the wall

`core/src/main/java/com/legend/lowering/Scalars.java:2585`
`static SqlExpr pureToString(Type t, SqlExpr x)`. Its complete arm list, in
evaluation order:

| line | arm |
|---|---|
| 2586-2588 | `Type.Primitive.FLOAT` → `floatRepr(x)` |
| 2589-2627 | `Any` (variant-carried): LITERAL wire → `literalPrint(x)`; else JSON ARRAY / VARIANT_GET |
| **2628-2638** | `PlatformTypes.isListCarrier(t)` → `'[' ‖ joinList(list_transform(x, e → pureToString(elem, e))) ‖ ']'` — a hand-inline of `anonymousCollections.pure:36-39` |
| **2639-2650** | `PlatformTypes.isPairCarrier(t)` → `'<' ‖ pureToString(first) ‖ ', ' ‖ pureToString(second) ‖ '>'` — a hand-inline of `anonymousCollections.pure:21-24` |
| 2668-2671 | `Nil` → `CAST(NULL AS VARCHAR)` |
| 2679-2683 | `Variant` → canonical JSON text |
| **2684-2694** | relation / FunctionType / SchemaAlgebra / **non-Any `ClassType`** → `throw new NotImplementedException("toString over " + t + " is not modeled")` ← **line 2692 is the 6a wall** |
| 2698-2700 | `BOOLEAN` → `BOOL_TO_TEXT` |
| 2702 | else → `CAST(x AS VARCHAR)` |

Registered from `Scalars.java:2148-2176`
(`for (String f : Pure.nativeKeysAt("toString"))`), which also handles DATE_TIME
(`STRFTIME` + `DateFmt.ISO_PURE_UTC`, **:2166-2169**) and FLOAT (**:2170-2172**).

**Reading the arm list against §a is decisive: the List and Pair arms at 2628/2639
are already the reference's `toString()` qualified-property bodies, hand-transcribed
for the two platform classes that declare one.** The wall at 2692 is the *general*
case of the same rule, unimplemented. There is no third mechanism to invent.

**Why `testComplexClassToString` never reaches its own `toString()` body.** We *do*
support qualified properties: `ClassDefinition.derivedProperties` → lifted to
`<owner>$prop$<name>` by `compiler/DerivedProps.java:38-60` (`SynthHat.PROP`,
`model/SynthHat.java:22-23`), compiled to `Property.Derived` at
`compiler/element/ClassCompiler.java:54-74`. Two dispatch sites exist:

- `compiler/spec/Typer.java:510` — parameterized qualifier call, **gated on
  `functionCandidates(af).isEmpty()`**;
- `compiler/spec/Typer.java:2765-2807` — zero-arg derived read, but only for an
  `AppliedProperty` (the `.prop` spelling).

`^ClassWithComplexToString(...)->toString()` parses as an `AppliedFunction` named
`toString`, and `functionCandidates("toString")` is **non-empty** (3 natives). So
`Typer.java:510` is skipped, the native wins, and lowering hits `Scalars.java:2692`.
**The reference does the opposite** — its native's *own body* looks up the
qualified property first (`ToString.java:60-67`).

**What we already have for `testPersonToString`.** Instance construction lowers to
a struct with the model's canonical layout: `lowering/Lowerer.java:2662-2728`
(`TypedNewInstance`), with `TypedCopyInstance` at **:2619-2661**.
`compiler/element/ClassLayouts.java:31` declares `SYNTHETIC_ID = "__id"`, and
**:41-80** (`layoutOf(ctx, t, withIdentity)`) appends it as `STRING[0..1]` for
keyless non-carrier classes. `Lowerer.java:2702-2706` mints it at the construction
site from `instanceIdOf` (`Lowerer.java:200-214`). The minter is
`core/src/main/java/com/legend/exec/InstanceIds.java:29-31`:

```java
public String idOf(Object node) {
    return ids.computeIfAbsent(node, k -> "i" + (ids.size() + 1));
}
```

⇒ today's ids are **`"i1"`, `"i2"`, …** — they satisfy neither
`startsWith('Anonymous_')` nor `startsWith('@_')`.

And it is **gated**: `StatementExecutor.java:2313-2329` only calls
`withInstanceIds` when `identity` is true, and **:2598-2599** passes
`rider != null || identityLane` — the verdict lane only. An ordinary
`let str = …` statement is not that lane, so `__id` is not even in the struct on
the path `testPersonToString` takes.

**The existing SQL-side instance renderer (the precedent the charter was looking
for).** `core/src/main/java/com/legend/lowering/CanonicalRenderSql.java` (558 lines):

- **:286-302** `identityCanon(SqlExpr v, String fqn, SqlType layout)` — reads
  `__id` **out of the struct in SQL** and emits
  `{"_type": fqn, "_id": <struct_extract(v,'__id')>}`, NULL-guarded.
- **:304-314** `instanceEqualityCanon` — keyed ⇒ `instanceCanon`, keyless ⇒
  `identityCanon`.
- **:331-…** `instanceCanon` — the keyed key-tree render, `Fqn(k1,k2,…)`, leaves
  kind-tagged, empty `[0..1]` key ⇒ `'[]'`, NULL instance ⇒ NULL.

**The print-form render owner.** `core/src/main/java/com/legend/lowering/Render.java`
(928 lines) already owns `lowerToCsv` (F4.2) and `lowerToString` (F4.2c — the
relation `#TDS` text, **:315-345**), dispatched from `Lowerer.java:2931-2940`.
`docs/CANONICAL_FORM_SPEC.md:25-28` names Render as the owner of both the scalar
and grid channels.

**Representation-rule owners (C2.5's named exemplars, verified).**
`Scalars.floatRepr` at `Scalars.java:3096-3098` is a **one-line delegate** to
`LiteralSpelling.floatPrint` (`lowering/LiteralSpelling.java:387`), which composes
DuckDB expressions (VARCHAR cast, HUGEINT plain path for fraction-free values,
`DECIMAL(38,18)` + `RTRIM`) — **rendered in the database**. `Scalars.literalPrint`
(**:3100-3105**) delegates to `LiteralSpelling.printForm`
(`LiteralSpelling.java:339-364`), also pure SQL (`SUBSTRING`/`REPLACE`/`STARTS_WITH`
CASE). `DateFmt` supplies the format literals (`Scalars.java:2168`, **:1742**).
**Both are SQL-expression builders, not Java string formatters.**

**Struct/NULL facts.** `sql/SqlExpr.java:601-624`:
`StructLit(List<Field>, TypeFact)`; `Field(String name, SqlExpr value, @Nullable SqlType declared)`.
`sql/dialect/DuckDb.java:311-318` `structLit` → `{'name': value, …}`; **:320-332**
`structFieldValue` → when `declared != null` **and** the value's `TypeFact` is
`Bottom`, emit `CAST(<expr> AS <declared>)`. So an unset optional property is
`CAST(NULL AS VARCHAR)` inside the struct — a **typed SQL NULL, not `''`**.

`Lowerer.java:2808`: `case TypedEnumValue e -> new SqlExpr.StringLit(e.value())` —
enum values are their bare member name as a string. `Scalars.java:2078-2115` lowers
`format(...)` to DuckDB `printf` with the array spread, already pre-printing
Pair/List-typed `%s` slots via `pureToString` (**:2107-2112**).

### 6b — the wall and the exact candidates

**The wall**: `core/src/main/java/com/legend/compiler/spec/InferenceKernel.java:954-956`

```java
throw new TypeInferenceException("ambiguous overload of '" + name + "': "
        + winners.size() + " candidates tie for the argument types");
```

inside `resolveOverload` (**:884-960**). `name` is
`candidates.get(0).qualifiedName()` (**:888**).

**ALL `toString` entries in our registry** —
`core/src/main/java/com/legend/builtin/Pure.java`, exhaustive:

| line | constant | signature | arity |
|---|---|---|---|
| **1742** | `TO_STRING__RELATION` | `meta::pure::functions::relation::toString<T>(rel:Relation<T>[1]):String[1]` | **1** |
| 1743 | `TO_STRING__RELATION_BOOL` | `meta::pure::functions::relation::toString<T>(rel:Relation<T>[1], typesAndMuls:Boolean[1]):String[1]` | 2 |
| **2227** | `TO_STRING__ANY_1` | `meta::pure::functions::string::toString(any:Any[1]):String[1]` | **1** |

(`planToString` / `planToStringWithoutFormatting` at **:1695**/**:1697** are
different bare names.) **At arity 1 there are exactly TWO candidates —
`relation::toString` and `string::toString` — matching the reported "2 candidates
tie" exactly. `relation::toString` is declared first, which is why it is the name
in the message.**

They are unioned by **bare name across packages** — `Pure.java:948-957` builds
`FN_BY_BARE` from every `NativeFunctionDefinition`, and `nativeFunctionsAt`
(`Pure.java:1064-1071`) documents it: *"FQN-keyed catalog with a BARE-NAME
secondary index … a bare lookup returns the union of overloads **across packages**
(overload resolution picks by shape)."* `Typer.functionCandidates` (**:2299-2319**,
**:2328-2350**) reads that index. **There is no repository/provenance dimension** —
this is the divergence from
`FunctionExpressionMatcher.getFunctionsWithMatchingName` + repo layering.

**ALL scoring criteria in our resolver, exhaustively** (`InferenceKernel.java`):

1. Arity filter (**:889-901**); 0 matches ⇒ `no overload of '…' accepts N argument(s)`;
   exactly 1 ⇒ chosen without scoring (**:902-904**).
2. `score(c, args)` (**:993-1009**) = Σ over params of
   `paramTypeScore * 20 + paramMultScore`; any `-1` ⇒ structural non-match.
3. `paramTypeScore` (**:1055-1090+**): unwrap Function-vs-FunctionType on both
   sides (**:1053-1060**); **`if (isNil(actual)) return 0;` (:1065-1067)**; then
   `formal` switch — `Any` ⇒ 0, `TypeVar` ⇒ 0, Primitive/PrecisionDecimal ⇒
   `primitiveTypeScore`, `TabularDataSet` vs relation ⇒ 1, `ClassType` ⇒ 2 exact /
   1 subtype / −1, `EnumType` ⇒ 2 exact / −1, **`GenericType` with
   `rawFqn == Relation` ⇒ `Type.isRelation(actual) ? 1 : -1`**, TDS ⇒ subtype-grade.
4. `paramMultScore` (**:1181-1208**): `Multiplicity.Var` ⇒ 9; `Bounded` equal ⇒ 10;
   else covariant containment check (fail ⇒ −1) then `multiplicityTightness`
   (**:1210-1225**) — `[1]`⇒8, `[0..1]`⇒6, `[1..*]`⇒4, `[*]`⇒…
5. Winner selection (**:904-925**); zero survivors ⇒ `no overload … structurally matches`.
6. Two tolerances before the throw: **duplicate-signature** (identical params +
   return + return-mult ⇒ first wins, **:928-940**) and **native-over-module**
   (exactly one native among the winners ⇒ that one, **:941-953**).
7. Otherwise ⇒ the throw at **:954-956**.

**The proof that this is Nil-specific.** Because `paramTypeScore`'s `Relation<T>`
arm returns `-1` for any non-relation actual, `relation::toString` can only survive
the structural filter when the actual **is** a relation or when the `isNil`
short-circuit at **:1065** fires first. A relation actual would score `1` vs `Any`'s
`0` and win outright. **Therefore the observed 2-way tie is reachable if and only
if the argument type is `Nil`** — which makes this categorically an
empty-collection problem, and confirms `$x : Nil[1]` in
`{x, y | $x->toString() == $y->toString()}` over `[]` without needing a probe. Both
candidates score `0*20 + <same mult score>`; not `allSameShape` (`Relation<T>` ≠
`Any`); both `isNative()` ⇒ `nativeWinners.size() == 2 ≠ 1` ⇒ throw.

**Corroborating prior measurements** (same mechanism, other names):
`docs/type-audit-2026-08/findings/A07-pure-registry.md:400-404` records
`[]->sum()` ⇒ 3 tie, `[]->max()` ⇒ 7 tie, `->sort([])` ⇒
`relational::sort: 2 candidates tie`; `docs/type-audit-2026-08/V1-falsifier.md:167`
grades the family "**CONFIRMED-BUT-OVERSTATED**".
`docs/INVENTION_AUDIT_2026_08_14.md:72-73` states the root shape outright: *"bare
name → ALL overloads across packages" — over the single `ALL` list. There is no
internal/external partition.*

**Is 6b a scoring problem or a type-inference problem? It is a CANDIDATE-SET
problem.** Not scoring (the reference's scoring ties identically — §a F-3). Not
type inference (`Nil` for `[]` is *correct*; `TypeMatch.java:394-397` and
`InferenceKernel.java:1065` agree that bottom conforms to everything, and both are
right). The divergence is that our registry admits a candidate real Pure's
repository graph does not contain.

---

## c) MINIMUM DESIGN — the decisions

### Which tenet clause each decision invokes

`docs/JAVA_EVICTION_PLAN.md:24-35` gives three clauses. **6a-Person invokes clause
3** — "Java that formats text **about** plans (metamodel TEXT) → the engine-parity
census decides; PERMANENT is a legitimate verdict" — *for the id VALUE only* (a
construction-site identity is model/plan text, not a query-time value), and
**clause 1 for everything downstream** (the id must travel as a struct field and be
concatenated/compared by the database, exactly as `CanonicalRenderSql.identityCanon:286-302`
already does). **6a-Complex invokes clause 1 outright**: it is ordinary query-time
evaluation of a user function body. **Clause 4 (`TENET_CHARTER.md:126-144`) is
unavailable to both**: an instance is a composite, not a bare literal node, so it
fails admission condition (1); and `JavaEvalLedgerTest`'s pins are shrink-only, so
no entry may be added.

**C2.5 (`TENET_CHARTER.md:56-59`) is engaged in general but not by these two
witnesses.** Neither `STR_Person` (`toString.pure:167-168`: two `String[1]`) nor
`ClassWithComplexToString` (**:179-180**: `ErrorType[1]`, `String[1]`) has a Float,
Date, or Decimal property. However, the *general* instance rule composes property
print forms, so any Java-side instance formatter would duplicate
`LiteralSpelling.floatPrint` and `DateFmt` — which C2.5 forbids. **This alone rules
out Java formatting for the general rule**, and is the necessity proof the design
does not need to supply because it never formats in Java.

### 6a — five decisions

**D1. `toString` over a non-primitive DISPATCHES; it does not format.** Implement
`ToString.java:52-69` verbatim as a compiler rule: for a `ClassType`/`GenericType`
receiver, look up a zero-arg derived property named `toString` on the class or a
generalization (`_Class.java:114-120`, our `ctx.findProperty` + the superclass walk
`ClassLayouts` already does); if found, rewrite the call to
`AppliedFunction(d.bodyFunctionFqn(), [receiver])` — the identical rewrite
`Typer.java:545-546` and `Typer.java:2803-2807` already perform. If not found, D3.
*This is not a new mechanism: it is the general form of the List/Pair arms at
`Scalars.java:2628` and `:2639`, and it deletes them as special cases.*

**D2. The dispatch site is the Typer, not the Lowerer.** `Typer.java:510`'s gate
`functionCandidates(af).isEmpty()` is the bug: it makes a registered native shadow a
class's own qualified property, which the reference never does. Narrow the gate so
that a **zero-arg qualified property whose name matches the call and whose owner is
the receiver's class** wins over a native — the reference's ordering
(`FunctionExpressionProcessor.java:290-392`: property route first, function library
only `if (foundFunctions.isEmpty())`, **:384-388**). Doing it at TYPE time means
`format`, `if`, and enum `equal` in the body reach lowering as ordinary expressions
that already work (`Scalars.java:2078-2115`, `Lowerer.java:2808`). **Zero new MIR
variants, zero new render arms** — Invariant 3a is not engaged.

**D3. The no-qualified-property arm renders the instance's `__id` IN SQL, in
`Render`/`CanonicalRenderSql`, never in Java.** The value already exists as a struct
field (`ClassLayouts.java:31`, minted at `Lowerer.java:2702-2706`), and
`CanonicalRenderSql.identityCanon:286-302` already extracts it in SQL. The print
form is `struct_extract(v, '__id')` — a column read plus at most a concatenation,
emitted by the one compiler and rendered by the dialect (Invariant 3). **No Java
string is composed from any property value.**

**D4. The id SPELLING moves to the reference's, and the spelling gets ONE owner.**
`InstanceIds.java:30` must produce a string satisfying
`ModelRepository.isAnonymousInstanceName` — `"@_"` + the site counter in base 32,
left-zero-padded to 7 (`ModelRepository.java:902-916`). This is a **model-text
constant computed at plan-build time** (clause 2 of `JAVA_EVICTION_PLAN.md:28-33`)
and is *already* a registered Java surface (`JavaEvalLedgerTest` lists
`InstanceIds.java` in `EXEC_CLASSES` with the justification "*the id EMITS into SQL
as a struct literal (the database still computes every compare over it)*") — so no
ledger entry is added and no pin is loosened. It is not an invention: the format is
transcribed from `ModelRepository.java:85-86, 902-916`.

**D5. The identity layout must reach the ordinary value lane.** Today
`withInstanceIds` is armed only when `rider != null || identityLane`
(`StatementExecutor.java:2598-2599`, gate at **:2327-2329**). `testPersonToString`'s
`let` is neither. Decide **either** (a) arm the identity layout wherever a
class-typed value can be `toString`-ed, accepting the layout widening (and its
`attr-count` consequences, `ClassLayouts.java:50-57`), **or** (b) make the id a
*lowering-time* property of the construction node read directly by the toString
rule, without widening the struct. (b) is smaller and does not perturb any pinned
golden text; (a) is more uniform. **This is the one genuinely open design choice in
6a** and it needs a live probe (§e).

### 6b — three decisions

**D6. Do NOT touch the scoring, and do NOT add a tiebreak.** Verified: the
reference's scoring produces `.equals()`-identical matches here
(`TypeMatch.java:78-101, 394-397`; `GenericTypeMatch.java:236-239`;
`FunctionMatch.java:68-103`). Any discriminator we invent between `Any[1]` and
`Relation<T>[1]` against `Nil[1]` would be **a value the platform never computed** —
C2.4 (`TENET_CHARTER.md:52-55`), and "'fixing' a fallback by changing what it
defaults to" (AGENTS.md common mistake #7). Equally, do not delete
`TO_STRING__RELATION`: it is a real engine function
(`core_functions_relation/relation/functions/toString.pure:19`) with a live render
path (`Lowerer.java:2931-2940` → `Render.lowerToString:315`).

**D7. Give native functions a PROVENANCE/layer, and scope bare-name candidates by
it.** This *is* the engine's real rule —
`FunctionExpressionMatcher.getFunctionsWithMatchingName:153-157` + repository
layering, with `core_functions_relation` depending on `platform` and never the
reverse. `Pure.java:948-957` already partitions `FN_BY_BARE` by one predicate
(`userResolvable`, excluding the lite package per
`LITE_INVENTION_CENSUS.md:136-141`); adding a repo/layer field to
`NativeFunctionDefinition` and filtering `FN_BY_BARE` by the compiling source's
layer is the same machinery, generalized. This is Invariant 1's "fix the frontend"
(AGENTS.md:142-158) and passes C1.6's litmus (`TENET_CHARTER.md:36`) — pure
model-space compilation, no database. **It fixes the whole family at once**
(`[]->sum()` 3-way, `[]->max()` 7-way, `->sort([])` 2-way —
`A07-pure-registry.md:400-404`), not one row.

**D8. If D7 is judged too large for this leg, the honest fallback is to keep the
throw and adjudicate the row — not to break the tie.** `Nil`-argument ambiguity is
*real* under a flat registry; a loud `TypeInferenceException` is C2.4-correct. What
is **not** acceptable is an arbitrary winner. The scope of D7 is bounded by a
measurable fact: at arity 1 the collision set for `toString` is exactly 2
(`Pure.java:1742`, **:2227**).

---

## d) TRAPS

**T1 — `Lowerer.java` is at 3500/3500.**
`core/src/test/java/com/legend/CodeShapeGuardrailTest.java:35` sets
`FILE_LIMIT = 3500`; **:42-47** shows `FILE_ALLOWLIST` contains **only**
`MappingNormalizer.java → 3510`. Measured now: `Lowerer.java` **3500** (zero
headroom), `Scalars.java` **3424** (76), `InferenceKernel.java` 1449,
`Render.java` 928, `CanonicalRenderSql.java` 558, `LiteralSpelling.java` 472.
`METHOD_LIMIT = 250`. **Put D3's arm in `Render.java` or `CanonicalRenderSql.java`;
put D1/D2 in `Typer.java`. Deleting the List/Pair arms (`Scalars.java:2628-2650`)
frees ~22 lines there.**

**T2 — the id spelling change is observable in three places.**
`InstanceEquality.java:119-121, 196` compares `__id` to `__id` (spelling-agnostic —
safe). `CanonicalRenderSql.identityCanon:286-302` emits the id into the
`{_type,_id}` JSON canon that byte-verdicts compare — **both sides change together,
so verdicts hold, but any pinned canon TEXT would move.** `AssertVerdicts.java:594`
and `exec/PureAsserts.java:171` also read `SYNTHETIC_ID`. Grep every golden
containing `"i1"`/`"_id"` before changing `InstanceIds.java:30`.

**T3 — `record`-equality is the id key.** `InstanceIds` keys an `IdentityHashMap`
on the AST **node object** (`InstanceIds.java:23, 29-31`), justified at **:10-15** by
the inliner's "verbatim let splicing / untouched subtrees keep identity" contract.
Two *structurally equal* `TypedNewInstance` records that are distinct objects get
distinct ids; one node reached twice gets one id. `JavaEvalLedgerTest.java:264-274`
already records a decline for exactly this hazard ("*a keyless ctor under a lambda
mints ONE site id for many evaluations — decline, counted*"). `testPersonToString`
constructs once in a `let` — safe — but any inliner change that re-instantiates the
node changes the id. **Do not switch the map to value-equality.**

**T4 — NULL vs empty string.** `DuckDb.structFieldValue` (**:320-332**) emits
`CAST(NULL AS <declared>)` for an unset field. In DuckDB, `'a' || NULL` is NULL, so
a naive concatenation over a struct would make the whole print NULL. The reference
prints `"NULL"` for a null in `CompiledSupport.java:1191-1194`, and `''` in the
**grid** channel only (`CANONICAL_FORM_SPEC.md:49`). `CANONICAL_FORM_SPEC.md`'s §2
scalar table has **no NULL/empty row at all** — an unresolved spec gap. *D1/D3
sidestep it entirely* — **but any future general property-concatenating printer must
resolve it first.**

**T5 — float print must not be re-implemented.**
`docs/CANONICAL_RENDER_HOMEWORK.md:74` records the measured divergence: DuckDB's
bare cast of `0.000000013421` gives `1.3421e-08`; pure gives `0.000000013421`.
`LiteralSpelling.floatPrint` (**:387**) is the fix and the only owner. Reaching a
Float property from a Java-side instance printer would reintroduce the divergence
and violate C2.5. Same for dates (`DateFmt`, `Scalars.java:2166-2169`) — our own
oracle at `EssentialFunctions_manifest.duckdb.json:433-435` shows the bare-cast
failure mode: `expected '2014-01-01T00:00:00.000+0000'` / `actual '2014-01-01 00:00:00'`.

**T6 — stale charter/doc assumptions, itemized.**

- §3's "package-qualified name + properties **or** the @anonymous form" — **the
  first half does not exist** (F-1). Only `toRepresentation` uses a qualified path,
  and only for `PackageableElement`s (`toRepresentation.pure:27`).
- §3's "ONE owner, presumably beside LiteralSpelling/printForm" — for
  `testComplexClassToString` there is **no print owner at all**; it is a resolution
  fix (F-2).
- §3's "fix scoring, not a special case" for 6b — **the reference's scoring ties
  too** (F-3); the fixable layer is the candidate set.
- `docs/CANONICAL_FORM_SPEC.md:89-90` ("*Class instances: identity ids are not
  canonical (H1) — instance equality stays a host/referee concern, out of the byte
  channel*") is **contradicted by §4b Tier B at :111-117** ("the print-form identity
  carrier" is CLAIMABLE) **and by shipped code** (`CanonicalRenderSql.identityCanon:286`,
  `instanceCanon:331`; `JavaEvalLedgerTest.java:255-274`, **:493-500**). §4 was never
  amended. Do not cite **:89-90** as authority.
- `docs/PCT_EXPECTED_FAILURES.md:44` ledgers `testComplexClassToString` as "*prints
  the metamodel form of a class instance*" under §A "*Instance identity & metamodel
  reflection — impossible over a SQL wire*". **That characterization is wrong** — the
  test never prints a metamodel form; it evaluates a user-declared function body. Per
  that file's own rule (**:140-141**, "*a ledger entry that stops failing is a FIX —
  delete the entry*"), the entry and
  `Test_LegendLite_EssentialFunctions_PCT.java:65` both come out when D1/D2 land, and
  the build **will fail loudly** if they are left in.
- `docs/DUCKDB_FUNCTION_COVERAGE.md` does **not** contain per-function rows for
  `printf`, `struct_extract`, `list_transform`, `concat`, or `CAST…VARCHAR`
  (verified: zero hits). It is a strategy doc (headline: a `RawFn(name,args)`
  passthrough leak on the aggregate/window paths, **:206**, **:227**). Do not use it
  as a capability oracle. Actual evidence the primitives exist: `DuckDb.java:311-318`
  (struct literal), **:331-333** (`struct_extract`), `Scalars.java:2078-2115`
  (`printf`), `Scalars.java:2633` (`LIST_TRANSFORM`), **:2571-2574** (`STRING_AGG`).
- `docs/CHANNELB_BURNDOWN_HANDOFF.md:55` bundles all three rows under one leg with
  two error strings. They are three different failures at two different layers.

**T7 — the reference relational manifests are the WRONG oracle for this leg.**
Channel B β-reduces the adapter and runs the whole body (`ChannelB.java:24-39`,
**:271-305**), so the bar is the interpreted/compiled platform (which has **no**
Essential exclusions at all). `ChannelBEssentialTest.java:39-40` pins discovery at
exactly 327 and **:59-61** pins `pass >= 305` — shrink-only floors, so a fix must
move the floor up, never the denominator down (**:26-29**).

**T8 — D7 changes candidate sets globally.** `Pure.java:948-957` feeds `FN_BY_BARE`
*and* `KEYS_BY_NAME`, which the lowering's rule tables consult via `nativeKeysAt`
(e.g. `Scalars.java:2148`). Layer-filtering must apply to **user resolution only** —
exactly the partition `INVENTION_AUDIT_2026_08_14.md:151-158` prescribes ("*The fix
is a partition, not a rewrite … have the user-facing resolution path consult an
index that excludes them, while the normalizer and lowering keep emitting them*"),
pinned by `NativeCatalogGovernanceTest` (`LITE_INVENTION_CENSUS.md:146-149`).

---

## e) CONFIDENCE + LIVE PROBES

**High confidence (read end-to-end in source, both repos):**

- The reference's complete `toString` rule for non-primitives, both runtimes
  (`ToString.java:52-69, 83-86`; `_Class.java:114-120`;
  `CompiledSupport.java:1189-1244, 1246-1302`).
- The exact expected strings and the absence of one for `testPersonToString`
  (`toString.pure:137-141, 154-157`), and the `%s`-not-`%r` fact that makes them
  unquoted (`PureStringFormat.java:53-64`).
- The anonymous-id format and both accepted prefixes
  (`ModelRepository.java:85-86, 902-916, 935-937`).
- The complete reference overload algorithm and every ordering criterion
  (`FunctionExpressionMatcher.java`, `FunctionMatch.java`, `TypeMatch.java`,
  `GenericTypeMatch.java` — all read in full).
- `coreImport` contains both packages (`m3.pure:192, 199`) ⇒ imports do not
  disambiguate; `core_functions_relation` depends on `platform` ⇒ layering does.
- Our two walls (`Scalars.java:2692`; `InferenceKernel.java:954-956`), the complete
  candidate list (2 at arity 1), and the complete scoring criteria.
- That the tie is reachable **only** for a `Nil` argument — proved from
  `paramTypeScore`'s `Relation<T>` arm returning `-1` otherwise.
- Every manifest row across all 15 reference adapters, and the absence of
  `core-interpreted`/`core-compiled` Essential manifests.
- File sizes and the guardrail (`CodeShapeGuardrailTest.java:35, 41-47`).

**Medium confidence — reasoned from source, not executed:**

- That `Typer.java:510`'s `functionCandidates(af).isEmpty()` gate is the *only*
  thing preventing `ClassWithComplexToString.toString()` dispatch. The chain
  (`Typer:510` skipped → native resolved → `Scalars:2692`) is consistent with the
  pinned message at `Test_LegendLite_EssentialFunctions_PCT.java:65`, but a second
  possible short-circuit was not traced.
- That real Pure would raise `Too many matches for toString(…)` if both candidates
  were simultaneously visible. Read directly off
  `FunctionExpressionMatcher.java:140-148` + `FunctionExpressionProcessor.java:210`,
  never executed.
- That the `%s` slots in `ClassWithComplexToString.toString()` lower cleanly through
  `Scalars.java:2078-2115` — all three are `String[1]`, so the class-typed pre-print
  at **:2107-2112** is not engaged.

### Probes needed before implementation (all read-only / single-test runs)

1. **Which of the two 6a rows produce which error today.** Run
   `ChannelBEssentialTest` and read `[chB] FAIL/ERROR …toString::testPersonToString`
   and `…testComplexClassToString`. `CHANNELB_BURNDOWN_HANDOFF.md:55` gives two
   messages for three rows without mapping them; the inference (both hit
   `Scalars.java:2692` with different `fqn=` tails) is unconfirmed for
   `testPersonToString`. *(Note: leg 7b's measured census independently confirms both
   rows carry the `NotImplementedException: toString over ClassType[...]` shape — see
   `leg7b.md` Appendix A1.)*
2. **Whether `__id` is in the struct on `testPersonToString`'s lane.** Run with
   `LL_TMP_SQL=1` and inspect the emitted `{…}` struct literal for a `'__id'` field.
   Reading of `StatementExecutor.java:2598-2599` + **:2327-2329** says **no** (a plain
   `let` is not the verdict lane) — this decides D5(a) vs D5(b) and is the single
   highest-value probe.
3. **Whether `$x`'s inferred type in the empty-list lambda is exactly `Nil[1]`.**
   `LL_TDG_DEBUG=1` prints `arg1 exprType=` at `InferenceKernel.java:1020-1024`. The
   `-1`-for-non-relation proof makes `Nil` certain; the *multiplicity* is not
   independently confirmed (irrelevant to the tie, since both formals are `[1]`).
4. **The full arity-1 collision census under D7.** Enumerate every bare name in
   `FN_BY_BARE` with ≥2 arity-matching overloads whose formals are
   `Any`/`TypeVar`/`Relation<T>`, to size the layer-filter blast radius before
   touching `Pure.java:948-957`.
5. **Goldens containing the id spelling.** `grep -rl` for `"__id"` and `_id` across
   `core/src/test/resources` and corpus goldens before changing `InstanceIds.java:30`.
6. **Whether removing `Test_LegendLite_EssentialFunctions_PCT.java:65` is required
   in the same commit.** That harness fails the build on an *unexpected pass*
   (`PCT_EXPECTED_FAILURES.md:4-9`), so Channel A and Channel B must move together.

---

## OPEN QUESTIONS

1. **D5: widen the identity layout, or read the site id without widening?** (a)
   arming `withInstanceIds` on the ordinary value lane touches
   `ClassLayouts.layoutOf(…, withIdentity)` consumers and the "attr-count wall"
   (`ClassLayouts.java:50-57`); (b) reading the id at the toString rule leaves the
   struct shape untouched. Probe #2 decides. **Not resolvable from source alone.**
2. **What should `toString` produce for a class instance whose class has no
   `toString()` AND no `__id`** — e.g. an instance read out of a mapped store? The
   reference always has an M4 name; a row-sourced instance has no identity at all.
   `ClassLayouts.java:55-57` says "*store-mapped reads would project NULL (no such
   producer exists in the identity lanes today — the attr-count wall guards it)*". A
   NULL id would make the print NULL. **Does this arm stay a loud
   `NotImplementedException`?** Probably yes, but nothing states it.
3. **Is `@_0000000` (interpreted) or `Anonymous_StripedId` (compiled) the right model
   for us?** Both satisfy the test. The compiled runtime's *constant* is strictly more
   deterministic and would make instance prints byte-stable across runs — attractive
   for the byte channel — but it destroys distinguishability between two instances,
   which `identityCanon:286-302` relies on for `eq`. **These two consumers want
   different things and the conflict is unadjudicated.**
4. **Does D2's resolution-order change (qualified property beats native) break any
   existing call?** Real Pure's ordering is property-first
   (`FunctionExpressionProcessor.java:290-392`), so it is spec-faithful — but our
   registry contains natives real Pure does not have (the shim family at
   `LITE_INVENTION_CENSUS.md:74`: `join`, `hash`, the ordering comparisons). A user
   class declaring a property named `join` would now shadow the shim. **Needs the D7
   partition or an explicit scope restriction.**
5. **Is D7 in scope for this leg, or is it its own leg?** It fixes `[]->sum()`,
   `[]->max()`, `[]->min()`, `[]->mode()`, `->sort([])` and `toString` together
   (`A07-pure-registry.md:400-404`) — high value, but it changes the shape of
   `NativeFunctionDefinition` and `FN_BY_BARE`, which `NativeCatalogGovernanceTest`
   pins. **Sizing depends on probe #4.**
6. **Should `CANONICAL_FORM_SPEC.md` §4 (`:89-90`) be amended in this leg?** It
   contradicts §4b (**:111-117**) and the shipped `CanonicalRenderSql` instance
   canons. Leaving it stale means the next reader is misled about whether instances
   may enter the byte channel. **A doc-only decision, but it is the adjudication
   authority for this leg's D3.**
7. **`CANONICAL_FORM_SPEC.md` §2 has no scalar NULL/empty row.** T4 shows this
   matters for any general property-composing printer. **Out of scope if D1/D3 are
   adopted; blocking if a different design is chosen.**
