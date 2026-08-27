# Leg 7b — the grammar walls

**Charted as:** 4 Grammar ERROR rows + 3 blocked Essential discovery rows, caused
by file-level parse walls in `new.pure`, `addColumns.pure`, `getUnitValue.pure` /
`newUnit.pure`, `cast.pure`, `toMultiplicity.pure`.

> ## HEADLINE — LEG 7b AS CHARTERED BANKS **ZERO** ROWS
>
> - The six parse-wall files contain **no `<<PCT.test>>` function at all**, so
>   burning every one of them moves PASS by 0 and discovery by 0.
> - The **"3 essential discovery rows" never existed** — a `PCT.test`
>   string-match miscount (two comments plus one Profile whose name merely
>   *starts with* `PCT.test`).
> - The **4 Grammar ERROR rows are not parse walls**: two are a type-layer
>   `@ExtendedInteger` failure, two are unrelated.
> - The only row-bearing fix in the neighbourhood is **~2 lines in
>   `PureModelContext.elementFqns()`** — not parser work.
>
> Leg 7b is a **drop-in-parity / corpus-integrity** leg, not a burndown leg, and
> should be scheduled and justified as one.

See `README.md` for the shared tenet quick-reference and provenance notes.

---

## 0. The two highest-value claims, evidenced

### 0.1 The six parse walls contain NO `<<PCT.test>>` function

**The discovery predicate.** `ChannelB.run` (`ChannelB.java:163-177`) iterates
model elements, keeping those that are a `FunctionDefinition` satisfying
`isPctTest(fd)` whose `elementSources` entry starts with a scope prefix.
`isPctTest` is `ChannelB.java:181-186`:

```java
return fd.stereotypes() != null && fd.stereotypes().stream()
        .anyMatch(s -> "test".equals(s.stereotypeName())
                && (s.profileName().equals("PCT")
                        || s.profileName().endsWith("::PCT")));
```

A row is discovered **only** for profile `PCT`, stereotype `test`.

**Per-file counts across all six wall files** (exhaustive, no sampling):

| wall file | `PCT.test` | `test.Test` |
|---|---|---|
| `grammar/functions/lang/creation/new.pure` | **0** | 33 |
| `essential/meta/type/relation/addColumns.pure` | **0** | 2 |
| `essential/lang/cast/cast.pure` | **0** | 11 |
| `essential/lang/cast/toMultiplicity.pure` | **0** | 11 |
| `essential/lang/unit/getUnitValue.pure` | **0** | 2 |
| `essential/lang/unit/newUnit.pure` | **0** | 3 |

Every test in these files is `<<test.Test>>` — profile `test`, stereotype `Test`
— which `isPctTest` rejects. These are legend-pure's own suite, not the PCT suite.

**The 330-vs-327 gap dissolved.** `ChannelBEssentialTest.java:43-44` attributes a
3-row loss to walls. 330 is the count of raw occurrences of the *string*
`PCT.test` under `essential/`. Subtracting the three non-declarations:

- `essential/tests/surveyor.pure:123` — `// PCT test runner — discovers <<PCT.test>> functions, injects adapter, handles exclusions` — a **comment**
- `essential/tests/surveyor.pure:189` — `// Recursively walk a package's children collecting functions with the <<PCT.test>> stereotype.` — a **comment**
- `essential/tests/pct_core.pure:23` — `Profile <<meta::pure::test::pct::PCT.testQualifierProfile>> meta::pure::test::pct::PCTCoreQualifier` — `PCT.test` is a **prefix** of `PCT.testQualifierProfile`; this is a Profile, not a test

330 − 3 = **327 real declarations = exactly the 327 discovered.**

**Cross-checked by set difference, not arithmetic.** The declared-FQN set was
extracted from the corpus and the discovered-FQN set from the live surefire XML,
diffed both directions:

- declared − discovered = `{meta::pure::test::pct::PCTCoreQualifier}` **and
  nothing else** (the Profile false-positive above)
- discovered − declared = **∅**

**Grammar lane, same check:** 137 raw `PCT.test` occurrences under
`grammar/functions` = **137 discovered**, both set differences empty. `new.pure`
contributes 0.

**Third, independent confirmation:** the 22 measured non-PASS Essential rows
(Appendix A1) contain not one row from any wall file. The zero yield is visible
from the row list alone.

> **Conclusion: burning all six wall files moves `327` by 0, `137` by 0, PASS by
> 0. It moves only the wall count, 19 → 13.**

### 0.2 The `PureModelContext.elementFqns()` fix — the only row-bearing item

**Why `testEqPrimitiveExtension` / `testEqualPrimitiveExtension` fail today.**
Traced through five files:

1. **Test source** — `grammar/functions/boolean/equality/eq.pure:143-151`, under
   `import meta::pure::functions::boolean::tests::equalitymodel::*;` (`eq.pure:15`):

```
function <<PCT.test>> meta::pure::functions::boolean::tests::equality::eq::testEqPrimitiveExtension<Z|y>(...)
{
    assert($f->eval(|eq(1, 1->cast(@ExtendedInteger))));
```

   `@ExtendedInteger` is a **simple** name needing import resolution.

2. **The declaring file parses fine.**
   `grammar/functions/boolean/testModel.pure:64`:

```
Primitive meta::pure::functions::boolean::tests::equalitymodel::ExtendedInteger extends Integer
```

   The plain, no-type-variable form that `ElementParser.primitiveElement()`
   (`ElementParser.java:1255`) handles. **That file is not in the wall list.** So
   this is not a parse wall.

3. **Resolution** — `ChannelB.java:235-236` calls
   `NameResolver.resolveQuery(lambda, imports, ctx.elementFqns())`.
   `NameResolver.resolveQuery` (`NameResolver.java:426-432`) builds `known` =
   native class FQNs + native enum FQNs + `modelFqns`; simple names are rewritten
   **only** against `known`.

4. **The hole** — `PureModelContext.elementFqns()`
   (`PureModelContext.java:145-162`) adds classes, enums, associations, mappings,
   legacyMappings, databases, runtimes, functions, and native enums. **It never
   adds primitive extensions.**

5. **Registration exists but is orphaned** — `ModelBuilder.java:278-279`:

```java
case com.legend.model.PrimitiveExtensionDefinition pe ->
        mb.primitiveExtensions.put(pe.qualifiedName(), pe.baseTypeName());
```

   This arm alone skips the `mb.internElement(registered, …)` call that every
   sibling arm in that switch makes.

6. **Lookup is FQN-exact by design** — `TypeClassifier.findType`
   (`TypeClassifier.java:32-49`) *does* consult `model.findPrimitiveExtension(fqn)`
   at **:39-42**. But `ModelBuilder.findPrimitiveExtension`
   (`ModelBuilder.java:413-428`) is documented **`EXACT-FQN lookup only`**, on the
   stated precondition *"references resolve to FQNs in NameResolver (extensions
   are in knownFqns)"* — **a precondition `elementFqns()` does not satisfy.** The
   comment adds that fuzzy matching here "would be the banned suffix-match
   pattern", so relaxing the lookup is the wrong end to fix.

7. **The throw** — `Typer.java:3098-3099`:

```java
.orElseThrow(() -> new TypeInferenceException(
        "unknown type '" + name + "' in @" + name));
```

   The measured message is `unknown type 'ExtendedInteger' in @ExtendedInteger`.
   **That it prints the *simple* name is itself the proof that step 3 never
   rewrote it** — had resolution worked, the message would carry the FQN.

**The ~2 lines.** In `PureModelContext.elementFqns()` (`:145-162`), alongside the
existing `model.classes().forEach(...)`, add:

```java
model.primitiveExtensionFqns().forEach(out::add);
```

`primitiveExtensions` is package-private in `com.legend.compiler`
(`ModelBuilder.java:402`) while `PureModelContext` lives in
`com.legend.compiler.element`, so this needs one public accessor:

```java
public java.util.Set<String> primitiveExtensionFqns() { return primitiveExtensions.keySet(); }
```

Two lines. **Not parser work.** It is the only identified change in leg 7b's
neighbourhood that banks rows: 2 of the 4 remaining Grammar ERROR rows. The same
hole is why `cast.pure`'s `@P8`/`@P(8)`/`@NP(8)`/`@OP8`/`@StringP` would still
fail even after wall 4's parse fix — all are simple names under
`import meta::pure::functions::lang::tests::cast::*;` (`cast.pure:16`).

**The 4 Grammar ERROR rows, measured verbatim:**

```
ERROR …getAll::testBasic :: MappingResolutionException: class query requires an
      execution context: add ->from(mapping, runtime) or supply a runtime
ERROR …equality::eq::testEqPrimitiveExtension :: TypeInferenceException:
      unknown type 'ExtendedInteger' in @ExtendedInteger
ERROR …equality::equal::testEqualPrimitiveExtension :: TypeInferenceException:
      unknown type 'ExtendedInteger' in @ExtendedInteger
ERROR …string::tests::plus::testPlusInIterate :: IllegalStateException:
      a scalar query has no row scope for $p.lastName
```

Rows 1 and 4 are unrelated to leg 7b entirely. Rows 2 and 3 are what R0 targets.
**None is caused by a parse wall.**

---

## a) REFERENCE SEMANTICS

Two oracles, and **they disagree** — that is the leg's central fact.

- **legend-pure M3:** `…/legend-pure-m3-core/src/main/antlr4/org/finos/legend/pure/m3/serialization/grammar/m3parser/antlr/core/M3CoreParser.g4`
  (622 lines; `M3Parser.g4` is a 7-line shim that imports it) and `M3CoreLexer.g4`.
- **legend-engine:** `…/legend-engine-language-pure-grammar/src/main/antlr4/org/finos/legend/engine/language/pure/grammar/from/antlr4/core/M3ParserGrammar.g4`
  (288 lines) + `…/domain/DomainParserGrammar.g4` (255) + `…/domain/DomainLexerGrammar.g4` (41).

Both reference checkouts are **git-clean** (legend-pure at `d00cfd5ba`,
legend-engine at `943d38b3dc2`) — every quoted line is genuine upstream.

**All wall messages and positions below are MEASURED**, from surefire artifacts at
`/Users/neema/legend/legend-lite/pct/target/surefire-reports/` (mtime Aug 27
17:44). An independent static trace of the parser predicted all six line:column
pairs exactly before the log was found. Measured header lines:

```
[chB] walls=19        [chB] census={PASS=305, FAIL=13, ERROR=9} total=327
[chB-gram] walls=19   [chB-gram] census={PASS=133, ERROR=4} total=137
```

19 walls, not 20: 7 parse walls + 12 MODEL-walls. Leg 7b owns 6 of the 7 parse
walls (the 7th, `grammar/m3.pure:[15:10] top-level ^Instance must be followed by
(...)`, is a different construct).

### Wall 1 — `new.pure [113:72] expected BRACE_OPEN but found PAREN_OPEN ('(')`

**Offending line, verbatim** (`platform/pure/grammar/functions/lang/creation/new.pure:113`):

```
Class meta::pure::functions::lang::tests::new::MyClassWithTypeVariables(x:Integer[1])
```

Column 72 is exactly the `(` — verified by character offset (the line is 85
chars). Construct: **class-level TYPE VARIABLES**.

Its consumer is a *second, distinct* construct, at `new.pure:126-131`:

```
   assertEquals('110', ^MyClassWithTypeVariables(10)(text = ['a', 'b']).res());
   assertEquals('14', ^MyClassWithTypeVariables(4)().res());
```

— **`expressionInstance` with `typeVariableValues`**. Also `new.pure:148`
(`Class …ClassWithExplicitType(x:Integer[1])`) and `:155`
(`^…ClassWithExplicitType(1)().prop1()`).

**legend-pure M3 — ACCEPTS.** `M3CoreParser.g4:33`:

```
classDefinition: documentation? CLASS stereotypes? taggedValues? qualifiedName typeVariableParameters? typeParametersWithContravarianceAndMultiplicityParameters?
       ( ( PROJECTS projection ) | ( ( EXTENDS type (COMMA type)* )? constraints? classBody ) )
;
```

`M3CoreParser.g4:47`:

```
typeVariableParameters: GROUP_OPEN (functionVariableExpression (COMMA functionVariableExpression)*)? GROUP_CLOSE
;
```

(`functionVariableExpression: identifier COLON type multiplicity`, `:462`.)

`M3CoreParser.g4:365-372`:

```
expressionInstance: NEW_SYMBOL (variable | qualifiedName)
                          (LESSTHAN typeArguments? (PIPE multiplicityArguments)? GREATERTHAN)? (identifier)?
                          (typeVariableValues)?
                          GROUP_OPEN expressionInstanceParserPropertyAssignment? (COMMA …)* GROUP_CLOSE
;
```

`M3CoreParser.g4:483`: `typeVariableValues: GROUP_OPEN (instanceLiteral (COMMA instanceLiteral)*)? GROUP_CLOSE`.

**legend-engine — SPLIT.** `DomainParserGrammar.g4:50` has **no**
`typeVariableParameters`. A grep for `typeVariableParameters` across **every**
`.g4` in legend-engine returns **0 hits**. But the *instantiation* side IS
engine-supported — `M3ParserGrammar.g4:162-168` carries `(typeVariableValues)?` at
line 164, defined at line 248.

### Wall 2 — `addColumns.pure [22:35] expected type name after '@'`

**Verbatim** (`platform/pure/essential/meta/type/relation/addColumns.pure:22`):

```
    let newRelation = addColumns(@(x:String)->genericType().rawType->cast(@RelationType<Any>)->toOne(), ~[ab:String[1], z:Integer]);
```

`@` is col 34, `(` is col 35. Construct: **`@` followed by an inline RELATION-TYPE
literal** (not a named type).

**legend-pure M3 — ACCEPTS.** `M3CoreParser.g4:312-322`:

```
atomicExpression: dsl | instanceLiteralToken | expressionInstance | unitInstance | variable
                 | columnBuilders
                 | (AT (type | multiplicity))
                 | anyLambda | instanceReference
;
```

and `type` alternative 3, `M3CoreParser.g4:465-481`:

```
type: ( qualifiedName (LESSTHAN (typeArguments? (PIPE multiplicityArguments)?) GREATERTHAN)?) typeVariableValues?
      | ( CURLY_BRACKET_OPEN functionTypePureType? (COMMA functionTypePureType)* ARROW type multiplicity CURLY_BRACKET_CLOSE )
      | ( GROUP_OPEN columnType (COMMA columnType)* GROUP_CLOSE )
      | unitName
;
```

**legend-engine — ALSO ACCEPTS.** `M3ParserGrammar.g4:123` `| (AT type)`; `type`
alt 3 is `relationType` (line 231), defined **:237-240**:

```
relationType :  PAREN_OPEN columnInfo (COMMA columnInfo)* PAREN_CLOSE ;
columnInfo: columnName COLON type multiplicity? ;
```

### Wall 3 — `getUnitValue.pure [23:36]` / `newUnit.pure [23:20]` — UNITS

Both `expected ')' to close argument list`.

**Verbatim** (`platform/pure/essential/lang/unit/getUnitValue.pure:23`):

```
    assertEquals(5, getUnitValue(5 RomanLength~Pes));
```

`RomanLength` starts at col 36.

**Verbatim** (`platform/pure/essential/lang/unit/newUnit.pure:23`):

```
    assertEquals(5 RomanLength~Pes, newUnit(RomanLength~Pes, 5));
```

`RomanLength` starts at col 20.

Construct: the **UNIT INSTANCE LITERAL** — a numeric literal *juxtaposed* with a
unit name. The *other* forms on the same lines (`newUnit(RomanLength~Pes, 5)`) are
bare unit-name references and already work.

The measure itself, `platform/pure/essential/meta/testModel.pure:65-71`:

```
Measure meta::pure::functions::meta::tests::model::RomanLength
{
    *Pes: x -> $x;
    Cubitum: x -> $x * 1.5;
    Passus: x -> $x * 5;
    Actus: x -> $x * 120;
    Stadium: x -> $x * 625;
}
```

That file is **not walled** — the Measure half already parses.

**legend-pure M3 — ACCEPTS.** `M3CoreParser.g4:316` lists `unitInstance` in
`atomicExpression`; **:195-199**:

```
unitInstance: unitInstanceLiteral unitName ;
unitName: qualifiedName TILDE identifier ;
```

**:444**: `unitInstanceLiteral: (MINUS? INTEGER) | (MINUS? FLOAT) | (MINUS? DECIMAL) | (PLUS INTEGER) | (PLUS FLOAT) | (PLUS DECIMAL)`.
Measure grammar at **:54-79**.

**legend-engine — ALSO ACCEPTS.** `M3ParserGrammar.g4:120` `| unitInstance`; lines
59-61 and 197 are token-for-token the same. `DomainParserGrammar.g4:129-146`
carries `measureDefinition`.

### Wall 4 — `cast.pure [39:54] expected EXTENDS but found PAREN_OPEN ('(')`

**Verbatim** (`platform/pure/essential/lang/cast/cast.pure:39-42`):

```
Primitive meta::pure::functions::lang::tests::cast::P(x:Integer[1]) extends Integer
[
    $this < $x
]
```

Col 54 is exactly the `(`. Construct: **`Primitive` declaration with TYPE
VARIABLES**, plus a constraint block referring to them.

**The charter's hypothesis for this wall is wrong.** It guessed `cast(x, @Type)`
or `<T|m>`. But `cast<T|m>` at `cast.pure:18` parses fine today — which is *why*
the error lands at line 39 and not line 18. Same shape at **:69** (`NP`) and
**:90** (`OP`).

Related constructs in the same file: **:95** —
`Primitive meta::pure::functions::lang::tests::cast::OP8 extends OP(8)` (type-variable
**application** in an `extends` clause); **:46** — `val : P(8)[1];` (application in
property-type position); **:51,56,66,81,86,101,106,113** — `->cast(@P(8))`,
`@NP(8)`, `@OP8`, `@StringP`.

**legend-pure M3 — ACCEPTS.** `M3CoreParser.g4:50-52`:

```
primitiveDefinition: documentation? PRIMITIVE stereotypes? taggedValues? qualifiedName typeVariableParameters? EXTENDS type
                     constraints?
;
```

Same `typeVariableParameters` (line 47) as `classDefinition` — **one shared
production**. `constraints` at **:386-403** includes `complexConstraint`
(`~function:` / `~message:`).

**legend-engine — REFUSES ENTIRELY.** There is no `PRIMITIVE` token in
`DomainLexerGrammar.g4` (all 41 lines read) and no `primitiveDefinition` in
`elementDefinition` (`DomainParserGrammar.g4:36-46`). Grep for `PRIMITIVE|Primitive`
across every engine `.g4` = **0 hits**. `ElementParser.java:737-744` already
records this, citing an adjudication probe.

### Wall 5 — `toMultiplicity.pure [34:46] expected type name after '@'`

**The charter recorded no message for this wall. This is it, MEASURED, not
inferred.**

**Verbatim** (`platform/pure/essential/lang/cast/toMultiplicity.pure:34`):

```
    assertEquals('a', ['a']->toMultiplicity(@[1]));
```

`@` is col 45, `[` is col 46. Construct: **`@` followed by a bare MULTIPLICITY
literal**.

The file's own comment (**:20-24**) names it:

```
// `@[m]` is the bare multiplicity literal: a single value whose *static
// multiplicity* is `m` and whose static type is `Any`.
```

Also `@[0..1]` (**:41,42**), `@[1..*]` (**:48,49,127**), `@[*]` (**:55,56**), and
`@[o]` with a **multiplicity parameter** (**:66**) under
`narrow<|o>(xs:String[*], hint:Any[o]):String[o]` (**:64**).

**legend-pure M3 — ACCEPTS.** `M3CoreParser.g4:319` — `| (AT (type | multiplicity))`.
**:498**: `multiplicity: BRACKET_OPEN multiplicityArgument BRACKET_CLOSE`;
**:532**: `multiplicityArgument: identifier | ((fromMultiplicity DOTDOT)? toMultiplicity)`.

**legend-engine — REFUSES.** `M3ParserGrammar.g4:123` is `| (AT type)` — **no
`multiplicity` alternative** — and `type` (lines 222-234) has none either.

### Oracle summary

| construct | legend-pure M3 | legend-engine |
|---|---|---|
| `Class X(x:Integer[1])` | ✅ `M3CoreParser.g4:33,47` | ❌ `DomainParserGrammar.g4:50` |
| `Primitive` element (any form) | ✅ `M3CoreParser.g4:50` | ❌ (no token, no rule) |
| `@(x:String)` | ✅ `:319`, `:474-478` | ✅ `M3ParserGrammar.g4:123,231,237` |
| `@[1]` | ✅ `:319`, `:498` | ❌ `M3ParserGrammar.g4:123` |
| `5 RomanLength~Pes` | ✅ `:316,195,198,444` | ✅ `M3ParserGrammar.g4:120,59,61,197` |
| `^X(10)(props)` | ✅ `:365-372,483` | ✅ `M3ParserGrammar.g4:162-168,248` |
| `Measure` | ✅ `:54-79` | ✅ `DomainParserGrammar.g4:129-146` |

---

## b) OUR SEAM

**Shared mechanics.** `TokenStreamCursor.expect(TokenType)`
(`TokenStreamCursor.java:243-252`) raises
`"expected " + type + " but found " + peek() + " ('" + safeText() + "')"` at
**:248**. `error(String)` (**:416-418**) → `throwAt(tokens(), pos(), msg)`
(**:391-405**), reporting the **cursor token's** 1-based line/column.
`Compiler.parseSources` (`Compiler.java:170-182`) catches per **file** and
`continue`s — one bad token discards the whole file. `ChannelB.run`
(`ChannelB.java:97-133`) then loops on `ModelException`, dropping whole files as
MODEL-WALLs.

### Wall 1 — TWO seams, both must land together

**1a — `ElementParser.parseClassDefinition` (`ElementParser.java:807`).**
Sequence: `expect(CLASS)` **:809** → `parseStereotypes()` **:810** →
`parseTaggedValues()` **:811** → `parseQualifiedName()` **:812** →
`parseClassTypeParams()` **:814** (returns immediately unless `peek()==LESS_THAN`,
**:900**) → `projects` check **:828** → `match(EXTENDS)` **:848** → constraints
**:859** → **`expect(TokenType.BRACE_OPEN);` at `ElementParser.java:863`**. With
the cursor on `(`, that is the measured `[113:72]`.

*Minimal delta:* an optional `typeVariableParameters` arm between **:814** and
**:828** — `PAREN_OPEN (functionVariableExpression (COMMA …)*)? PAREN_CLOSE`,
reusing `parseFunctionParameter()` (already called at `ElementParser.java:1520`).
Must be **carried, not dropped**: `Protocol.PClass` has a `typeParams` slot but no
type-variable slot.

**1b — `SpecParser.parseNewInstance` — a SILENT MISPARSE, not a missing feature.**
After `className = parseQualifiedName()`, an optional `<…>` via
`parseTypeArguments`, then
`expect(PAREN_OPEN, "expected '(' after class name or $variable in ^NewInstance")`.
There is **no `typeVariableValues` arm**. The body falls into the positional-cast
disambiguation:

```java
if (!className.isEmpty() && !isFqnSegmentToken(peek())
        && !(peek() == TokenType.STRING && … EQUAL)) {
    ValueSpecification src = parseCombinedExpression();
    expect(TokenType.PAREN_CLOSE, "expected ')' to close ^" + className + "($src) positional cast");
    return new AppliedFunction("new", List.of(receiver, new NewInstanceCast(className, typeArgs, src)));
}
```

`10` is an INTEGER, not an FQN segment → `^MyClassWithTypeVariables(10)` is
**silently reinterpreted as a positional cast**, yielding a wrong AST rather than
an error. The trailing `(text = […])` is then orphaned and fails one frame out at
`expect(PAREN_CLOSE, "expected ')' to close argument list")` (`SpecParser.java:1496`).

*Minimal delta:* a `typeVariableValues` arm before that `expect(PAREN_OPEN)`, using
a `PAREN_OPEN … PAREN_CLOSE PAREN_OPEN` lookahead — mirroring
`TokenStreamCursor.parseType:738-742`, which already calls
`parseTypeVariableValues()` for `Varchar(200)`.

**No third wall in `new.pure`.** All 457 lines were read.
`Class …MultiplicityParameterizedHolder<|m>` (**:286**) parses
(`parseClassTypeParams:908-918`); `^…<|1>(value='hello')` (**:293**) parses
(`parseTypeArguments:1757-1765`); `^$this(vals += …)` (**:190**) parses on
PLATFORM; property defaults (**:405,:425,:443**) parse.

### Wall 2 — `SpecParser.parseTypeAnnotation` (`SpecParser.java:2422`)

```java
private TypeAnnotation parseTypeAnnotation() {
    int atTok = pos;
    pos++; // consume '@'
    if (!isFqnSegmentToken(peek())) {
        throw error("expected type name after '@'");     // ← SpecParser.java:2426
    }
```

`isFqnSegmentToken` (`TokenStreamCursor.java:518-523`) is
`t != STRING && IDENTIFIER_TOKENS.contains(t)`; `PAREN_OPEN` is not in it. Cursor
on `(` → measured `[22:35]`.

**Already handled downstream — do NOT rebuild:** `@Relation<(…)>` (**:2448-2459**
→ `parseRelationShape` **:2498**), `@Type(200)` type-variable values
(**:2465-2474**), `@Mass~Kilogram` (**:2432-2438**), `@Generic<Args>` (**:2479-2486**).

*Minimal delta:* replace the `isFqnSegmentToken` gate with a dispatch —
`PAREN_OPEN` → the **existing** `parseRelationType()`
(`TokenStreamCursor.parseType:721-723` already routes it), wrapped in the existing
`TypeAnnotation.RelationShape` carrier. Nothing new to build behind it.

### Wall 3 — units: no seam exists

`SpecParser.parsePrimary` dispatches `case INTEGER -> parseInteger();`
(`SpecParser.java:700`), `FLOAT` (**:701**), `DECIMAL` (**:702**). `parseInteger`
(**:785-802**) consumes the token and returns `CInteger` with **no unit
lookahead**. Nothing anywhere implements `unitInstance`. The failure surfaces a
frame out at `parseArgListBody`'s
`expect(TokenType.PAREN_CLOSE, "expected ')' to close argument list")`
(**:1496**) — matching both measured messages exactly.

**Already supported — do NOT rebuild:**

- `Measure` declaration + protocol record — `ElementParser.parseMeasureDefinition:1544-1617`
  (canonical `*`, convertible/non-convertible fork, `PMeasure`/`PUnit`).
- Unit **name** reference — `SpecParser.parseQualifiedNameStart:1159-1166` folds
  `RomanLength~Pes` into one FQN string.
- Unit **type** in declarations — `TokenStreamCursor.parseType:730-734`.
- `@Mass~Kilogram` — `SpecParser.java:2432-2438`.

*Minimal delta (parse):* in `parsePrimary`'s three numeric arms, lookahead for
`identifier TILDE identifier` and build a unit-instance node.

*Delta (semantics) — THE TRAP:* grepping all of `com/legend` for `'~'` handling
finds **only** the parser string-folds (`TokenStreamCursor.java:732`,
`SpecParser.java:1165`, `:2436`) and two emitter checks
(`ProtocolEmitter.java:2045`, `:2098`). `TypeClassifier.findType` has no unit arm.
`TokenStreamCursor.java:726-729` states it outright: *"the classifier walls it as
an unported platform type until the units feature lands (parse-level coverage
only)"*. `newUnit` / `getUnitValue` / `unitType` / `unitValue` / `convert` are
unported natives.

### Wall 4 — `ElementParser.primitiveElement()` (`ElementParser.java:1255`)

```java
private PackageableElement primitiveElement() {
    advance();   // 'Primitive'
    String fqn = parseQualifiedName();
    expect(TokenType.EXTENDS);                        // ← ElementParser.java:1258
    String base = parseQualifiedName();
    // optional (args) on the base (e.g. Decimal(10,2)) — dropped
    if (peek() == TokenType.PAREN_OPEN) { skipBalancedBlock(); }
    // optional [constraints] — instantiation-time; dropped
    if (peek() == TokenType.BRACKET_OPEN) { skipBalancedBlock(); }
    return new com.legend.model.PrimitiveExtensionDefinition(fqn, base);
}
```

Cursor on `(` at **:1258** → measured `[39:54]`.

*Minimal delta:* insert `typeVariableParameters?` between **:1257** and **:1258** —
the **same helper** as wall 1a (M3 shares the production between
`classDefinition:33` and `primitiveDefinition:50`), so one helper serves both walls.

**Two silent-acceptance FALLBACK violations already live here and must be retired,
not extended:**

- **:1260-1263** — `skipBalancedBlock()` silently discards `extends OP(8)`
  (`cast.pure:95`), the only type-variable application in an `extends` clause in
  either reference tree.
- **:1264-1267** — `skipBalancedBlock()` silently discards the constraint block.
  `cast.pure`'s entire point is those constraints (`[$this < $x]` at **:41**;
  `[id(~function:$this < $x ~message:'the value is greater than '+$x->toString())]`
  at **:71** and **:92**); six of its eleven test functions assert the exact
  violation **message**.

`model/PrimitiveExtensionDefinition.java` is a two-field record
`(qualifiedName, baseTypeName)` whose own javadoc declares the drop: *"the
constraint block, when present, is parsed and dropped."*

### Wall 5 — SAME seam as wall 2

`SpecParser.java:2425-2427`. `BRACKET_OPEN` is not an FQN-segment token → the
identical `"expected type name after '@'"`, measured `[34:46]`. **Walls 2 and 5
are one line of code.**

*Minimal delta:* a `BRACKET_OPEN` arm calling the **existing**
`TokenStreamCursor.parseMultiplicity()` (**:1150-1178**), which already handles
`[1]`, `[0..1]`, `[1..*]`, `[*]`, and identifier parameters `[o]` →
`Multiplicity.Parameter` (**:1174-1177**). The real cost is a new `TypeAnnotation`
variant carrying a multiplicity, and every downstream `switch` over
`TypeAnnotation` gaining a real arm.

**Not walls, already supported:** `<T|m>` / `<|z>` / `<|o>` parameter lists —
`ElementParser.parseTypeAndMultiplicityParameters:2397-2419` skips the type side
when `peek()==PIPE`, called from `parseFunctionSignature:1510`. `Any[o]` /
`String[o]` — `parseMultiplicity:1174-1177`. So
`narrow<|o>(xs:String[*], hint:Any[o]):String[o]` (`toMultiplicity.pure:64`) is fine.

### Blast radius — exhaustive corpus counts

Both reference trees, `--include='*.pure'`; legend-pure = 275 files, legend-engine
= 3,180 files; `platform/pure` = 238 files. **Exact counts, not samples.**

| construct | legend-pure | legend-engine | total |
|---|---|---|---|
| `Class`/`Primitive` with type-variable params | 7 sites, 3 files | **0** | **7** |
| `Primitive` element, any form | 20 sites | 15 sites | **35** |
| — of which type-var application in `extends` | 1 (`cast.pure:95`) | 0 | **1** |
| `@(` inline relation type | 1 (`addColumns.pure:22`) | 3 (`testSchema.pure:16,22,39`) | **4** |
| `@[` bare multiplicity, **code** sites | 12 (`toMultiplicity.pure`) | **8, in 6 files** | **20** |
| unit LITERAL (`5 X~U`) | 20 lines, 2 files | **0** | **20** |
| `Measure` declarations | 1 | 4 | **5** |
| unit NAME refs (already supported) | 3 files | 24 files | 27 files |
| `^X(tvv)(props)` | 7, all `new.pure` | 0 | **7** |

Detail worth having:

- **The 7 type-variable declaration sites:** `new.pure:113,148`;
  `cast.pure:39,69,90`; `precisePrimitives.pure:44` (`Varchar(x:Integer[1]) extends String`)
  and **:61** (`Numeric(precision:Integer[1], scale:Integer[1]) extends Decimal`).
  The last two live outside `platform/pure`, so ChannelB never sees them — but they
  matter for drop-in.
- **The 12 `@[` code sites in `toMultiplicity.pure`** are lines
  `34,35,41,42,48,49,55,56,66,113,120,127`; the other 7 raw matches
  (`20,22,59,81,82,88,95`) are comment lines.
- **The 8 engine `@[m]` sites are ALL `->toMultiplicity(@[o])` inside PCT
  ADAPTERS:** `core_external_test_connection/pct_relational.pure:166`;
  `core_external_query_sql_reverse_pct/reverse_pct_adapter.pure:43`;
  `core_java_platform_binding/…/pct_java.pure:89`;
  `…pandas_api/pythonReversePCTPandasAPIApi.pure:28,29`;
  `…legend_ql/pythonReversePCTLegendQLApi.pure:29,30`;
  `core_deephaven_pct/pct_deephaven_adapter.pure:81`. **Every engine PCT adapter
  needs this form** — the strongest drop-in argument in the leg.
- **Unit LITERALS are legend-pure-only, 2 files.** The engine's own unit tests
  explicitly avoid them:
  `core_java_platform_binding/…/planConventions/test/unitLibraryTests.pure:38`
  carries `// TODO Does not work for literal units is used here` and uses
  `newUnit(Mass~Kilogram, 5.5)` instead.
- **`docs/parser-surface-exclusions.tsv` is HEADER-ONLY** (one line:
  `kind\tname\treason`) and `docs/c12-walls.tsv` is header-only too. **None of
  these five constructs is a declared exclusion.** Leg 7b is undeclared residue.

---

## c) MINIMUM DESIGN

**D1 — Correct the leg's premise before spending on it.** Leg 7b banks **0 rows**
as chartered. The 4 Grammar ERROR rows are not parse walls. This is a
**drop-in-parity / corpus-integrity** leg and should be scheduled and justified as
one. `docs/CHANNELB_BURNDOWN_HANDOFF.md:57` needs correcting.

**D2 — Split the leg by ORACLE, and gate accordingly.** Walls 2, 3, and the
`^X(tvv)` half of wall 1 are constructs the **engine accepts and we don't** —
plain parity debt, admissible on every dialect. Walls 1a, 4, and 5 are
**legend-pure-M3-only**; the engine parser refuses them. Those must be gated as
declared PLATFORM-dialect extensions, exactly as the codebase already gates class
generics (`ElementParser.java:815-821`, *"Type and/or multiplicity parameters are
not authorized in Legend Engine"*), `Primitive` itself (`ElementParser.java:742-744`),
and `^$var` copy-with-update. Ungated acceptance would make the "exact-engine"
surface accept more than the engine — a silent parity regression no current gate
can see.

**D3 — Parsing means MODELLING; where it cannot, do not parse.**

- **Units: do NOT parse-only.** The literal has no type, no lowering, no native.
  Accepting it moves the wall from PARSE to TYPE and banks nothing
  (`TokenStreamCursor.java:726-729` says so in the code). Either commit to unit
  semantics (Measure → Unit types, `newUnit`/`getUnitValue`/`convert` natives,
  unit-tagged carriers through the Lowerer — which is at its cap) or **declare it
  in `docs/parser-surface-exclusions.tsv`** and leave the wall. Half-doing it is
  the FALLBACK violation in reverse.
- **`Primitive` type variables: parse AND model, or don't touch it.** The method
  already commits two silent drops. Adding a third capability on top of two silent
  drops deepens the violation. `PrimitiveExtensionDefinition` needs a type-variable
  list and a constraint list, or the drops must become loud refusals.
- **`@[m]`: parse AND model.** A `TypeAnnotation` variant carrying a
  `Multiplicity` is cheap; the discipline is that every downstream `switch` gains a
  real arm, never a `default -> ignore`.
- **`@(cols)`: pure win.** `parseRelationType()` and the
  `TypeAnnotation.RelationShape` carrier both already exist and are already
  modelled for `@Relation<(…)>`.

**D4 — Wall 1b is a behaviour change, not an addition.** `^Class(10)` today
silently becomes a positional cast. Fixing it edits a live disambiguation on the
byte-parity path. Census `^Class(<non-identifier>)` across the parity corpus and
re-run the parity gates before and after.

**D5 — No file may grow past the cap.** `Lowerer.java` is at **exactly 3500**.
`SpecParser.java` is at **3440** — and walls 1b, 2, 3, and 5 *all* land in
SpecParser. Four features into 60 lines of headroom is not credible. **The
SpecParser seam split precedes leg 7b**, on the same reasoning the handoff already
applies to Lowerer.

**D6 — Move the pins with justification, and fix the stale comment.**
`walls <= 20` becomes `<= 13` if all six burn (measured 19 today). `== 327` and
`== 137` **do not move** — and the burn session must resist "fixing" them, because
the 330/327 delta was never real. Correct `ChannelBEssentialTest.java:41-44` in the
same commit.

### PASS-or-relocate, per wall — the decisive column

| wall | accepting the syntax… | net effect |
|---|---|---|
| **1a** `Class X(x:…)` | **relocates** PARSE → TYPE. `new.pure` holds 0 `PCT.test`; its 33 `test.Test` functions are invisible to ChannelB. Type variables then need modelling in `PClass`/`ClassDefinition`/`TypedClass` before `$x` resolves in the constraint at `:115` and the derived properties at `:118-120`. | wall −1, rows +0 |
| **1b** `^X(10)(props)` | **relocates**, and additionally **fixes a wrong AST** that today passes silently. | wall −0 (shares 1a), rows +0, correctness +1 |
| **2** `@(x:String)` | **relocates** PARSE → TYPE. Behind it sits `RelationType`/`ColSpec` metamodel reflection (`addColumns.pure:24-33` walks `$x.classifierGenericType.multiplicityArguments->at(0)`) — likely a further model-wall. **UNVERIFIED.** | wall −1, rows +0 |
| **3** units | **relocates** PARSE → TYPE, and the TYPE layer has *nothing* — no unit arm in `TypeClassifier.findType`, no natives, no lowering. The most clearly parse-only of the five. | wall −2, rows +0 |
| **4** `Primitive X(x:…)` | **relocates** PARSE → TYPE, into the *same* `elementFqns()` hole as R0 (all of `cast.pure`'s `@P8`/`@P(8)` are simple names under `import …::cast::*` at `:16`). Plus the constraint semantics its six message-asserting tests need. | wall −1, rows +0 |
| **5** `@[1]` | **relocates** PARSE → TYPE. Needs `toMultiplicity` as a multiplicity-narrowing native emitting the exact text `Cannot cast a collection of size N to multiplicity [BOUNDS]`. | wall −1, rows +0 |
| **R0** `elementFqns()` | **converts ERROR → PASS.** No parse change at all. | wall −0, **rows +2** |

**Every parse wall relocates. Only R0 converts.** That is the design conclusion.

### RANKED ORDER OF ATTACK

| # | item | seam | walls burned | rows banked | risk |
|---|---|---|---|---|---|
| **R0** | primitive-extension FQNs → `elementFqns()` | `PureModelContext.java:145-162` + accessor on `ModelBuilder.java:402` | 0 | **2** | very low, ~2 lines |
| **R1** | walls 2 + 5 (`@(…)`, `@[…]`) | `SpecParser.java:2425-2427` — one gate | 2 (19→17) | 0 | low; both delegate to existing helpers. Also unblocks all 8 engine PCT-adapter `@[o]` sites |
| **R2** | wall 4 (`Primitive` type vars) + retire the two silent drops | `ElementParser.java:1255-1269`; shared helper | 1 (→16) | 0 | medium; deletes two FALLBACK violations |
| **R3** | wall 1 (1a **and** 1b together) | `ElementParser.java:863` + `SpecParser.parseNewInstance` | 1 (→15) | 0 | **highest** — 1b edits a live disambiguation |
| **R4** | wall 3 (units) — **or declare it** | `SpecParser.java:785-802` + type layer + Lowerer | 2 (→13) | 0 | high cost, zero yield; smallest corpus footprint |

R0 is not parser work and is not in the charter — **do it first anyway**; it is the
only row-bearing item in the neighbourhood. After R1–R4, **12 of the remaining 13
walls are MODEL-WALLs** on m3 metamodel types (`Multiplicity` ×6,
`PackageableElement` ×2, `ValueSpecification` ×2, `FunctionType`, `Package`) — 63%
of the wall count, and a different leg.

---

## d) TRAPS

1. **The parse-vs-model trap, concretely: units.** Parsing `5 RomanLength~Pes`
   relocates the wall from PARSE to TYPE. `essential/lang/unit/*.pure` contains
   **zero** `PCT.test`, so nothing passes either way. A wall count dropping 19→17
   while nothing runs is exactly the "green check that only proves well-formedness"
   hazard.

2. **The Essential denominator will NOT grow — and that is the trap.** The charter
   and `ChannelBEssentialTest.java:43-44` both predict growth. It cannot happen: no
   wall file holds a `<<PCT.test>>`. A burn session that "fixes" `327` upward after
   burning walls would encode a miscount into a pin.

3. **Both `walls <= 20` pins move together.** `ChannelBEssentialTest.java:32-34`
   and `ChannelBGrammarTest.java:31-33` use the **same** `modelRoot = platform/pure`,
   so they share the same 19 walls. Ratchet both (`ChannelBEssentialTest.java:45`,
   `ChannelBGrammarTest.java:45`) or one silently keeps 8 walls of slack.

4. **Six pins at ZERO slack.** Measured today: essential `PASS = 305` against
   `>= 305` (**:78**); `AGREE-PASS = 293` against `>= 293` (**:164**);
   `WIRE-BUG = 9` against `<= 9` (**:165**); `DECLINED = 0` against `<= 0`
   (**:207**); `trueWireBug = 0` against `== 0` (**:237**); grammar `PASS = 133`
   against `>= 133` (`ChannelBGrammarTest.java:84`). **Any leg-7b change that flips
   one AGREE-FAIL row into a WIRE-BUG row fails the suite at three pins
   simultaneously.** A live hazard for R2 and R4. Re-measure the full lane after
   *each* of R1–R4, not at the end.

5. **Silent-acceptance fallbacks already in the tree.**
   `ElementParser.java:1261-1263` and **:1265-1267** `skipBalancedBlock()` past
   `extends OP(8)` and past every `Primitive` constraint block. Extending that
   method without retiring them makes the violation worse. `cast.pure`'s six
   constraint-message assertions are precisely the rows those drops would falsify.

6. **A live silent MISPARSE.** `^Class(10)` currently parses as
   `AppliedFunction("new",[ptr, NewInstanceCast(className, typeArgs, CInteger(10))])`
   — a wrong AST, not an error. **UNVERIFIED** whether any currently-green corpus
   row rides it; that census must precede the fix.

7. **File-size guardrails.** `lowering/Lowerer.java` = **exactly 3500** — nothing
   may grow it, which alone argues against attempting unit or `@[m]` *emission*.
   `parser/SpecParser.java` = **3440** and receives four of the six fixes (walls 1b,
   2, 3, 5) — **60 lines of headroom for four features**.
   `parser/MappingProtocolParser.java` = **3481** (untouched here, but next-closest
   to the cap). `parser/ElementParser.java` = 2749 (walls 1a, 4 — comfortable),
   `parser/TokenStreamCursor.java` = 1262 (the shared `typeVariableParameters`
   helper belongs here), `compiler/spec/Typer.java` = 3194,
   `compiler/ModelBuilder.java` = 1144, `compiler/element/PureModelContext.java` = 463.

8. **Record-equality / inliner hazards.** `PrimitiveExtensionDefinition` is a
   two-field `record`; adding type variables and constraints changes its equality
   and canonical form. `NameResolver.java:301-308` rebuilds it structurally on
   import resolution; `ModelBuilder.java:278-279` keys `primitiveExtensions` by FQN
   only. Any new field must thread all three. Likewise a new `TypeAnnotation`
   multiplicity variant changes the shape every `TypeAnnotation` `switch` sees, and
   `ProtocolEmitter.java:2045,2098` already sniff `'~'` in element-pointer paths —
   unit nodes must not silently change that emission.

9. **A green gate that cannot see these walls.** `docs/PARSER_DROP_IN_STATUS.md`
   §4.1b claims *"19,258 verdicts / 19,258 MATCH / 0 DIFF / 0 WALL / 0 PARSE_FAIL"*
   over the whole engine+pure checkouts. Not contradicted, and not reassuring:
   byte-parity is measured against the **engine** parser on engine-**comparable**
   elements, and `Primitive` / class type-variables / `@[m]` are outside that
   comparable set by construction. Absolute-zero parity and a live parse wall
   coexist.

10. **Stale charter assumptions, itemised.**
    - *"4 Grammar ERROR rows … are the parse walls"* — **false**; 2 are type-layer
      `@ExtendedInteger`, 2 are unrelated.
    - *"the walls also cost 3 essential discovery rows"* — **false**; it is 0, from
      a string-match miscount.
    - *"walls ≤ 20 … 20 measured"* — measured **19** today (7 parse + 12 model).
    - The charter's `cast.pure` hypothesis (`cast(x,@Type)` or `<T|m>`) — **wrong**;
      `cast<T|m>` at `cast.pure:18` parses fine; the wall is
      `Primitive P(x:Integer[1])` at **:39**.
    - The charter treats `new.pure` as one construct; it is **two** (class
      type-variable *declaration* at **:113** and instance type-variable *values* at
      **:126**), and both must land together or the file still walls.

---

## e) CONFIDENCE + LIVE PROBES

| wall | error site identified | confidence | still needs a live probe |
|---|---|---|---|
| **1a** `Class X(x:…)` | `ElementParser.java:863` | **Very high** — measured `[113:72]`; col 72 predicted from source before reading the log | that `PClass`/`ClassDefinition` can carry type variables without breaking byte-parity emission |
| **1b** `^X(10)(props)` | `SpecParser.parseNewInstance`, positional-cast branch | **High, UNVERIFIED live** — the file walls at `:113` before reaching `:126`, so the misparse is unobserved | census `^Class(<non-identifier>)` in the parity corpus; re-run parity gates around the disambiguation change |
| **2** `@(x:String)` | `SpecParser.java:2426` | **Very high** — measured `[22:35]`; col 35 predicted | whether `addColumns.pure` walls again *behind* this on RelationType/ColSpec metamodel reflection (`:24-33`) — **UNVERIFIED, likely deep** |
| **3** units | `SpecParser.java:1496` via `parseInteger:785` | **Very high** — measured `[23:36]` / `[23:20]`; both predicted | confirm the post-parse TYPE wall message; decide semantics vs declared exclusion |
| **4** `Primitive X(x:…)` | `ElementParser.java:1258` | **Very high** — measured `[39:54]`; col 54 predicted | whether `cast.pure` then walls on `@P8`/`@P(8)` (same `elementFqns()` hole as R0) — **highly likely** |
| **5** `@[1]` | `SpecParser.java:2426` | **Very high** — message **measured, not inferred**: `[34:46]`; col 46 predicted | which downstream `TypeAnnotation` consumers need a real multiplicity arm |
| **R0** primitive-ext typing | `PureModelContext.java:145-162` | **High on the CAUSE** — the error text prints the *simple* name, proving resolution never rewrote it | **UNVERIFIED that the fix makes the two rows PASS** — a type error masks whatever is behind it |

**Probe plan.** All parse-only where possible, so no execution machinery is needed.
After each fix, re-run **either** ChannelB suite and diff the `[chB-wall]` list —
the wall count is the instrument, since no row moves. **Do NOT use `-Dchb.only=`**:
the wall files hold no `PCT.test` to select, so a scoped run cannot observe these
fixes at all (and, per `ChannelB.java:156-160`, a scoped run trivially satisfies
the cumulative pins and writes no scoreboard).

For R0 specifically: run `ChannelBGrammarTest` and watch
`testEqPrimitiveExtension` / `testEqualPrimitiveExtension` move ERROR → PASS (or to
a new, more honest error). If they pass, ratchet `ChannelBGrammarTest.java:84` from
`>= 133` to `>= 135`.

---

## OPEN QUESTIONS

1. **Dialect gating.** `Primitive` type variables, class type variables, and `@[m]`
   are all refused by the engine grammar. Should they be accepted only on
   `LEGEND_PLATFORM` (like class generics at `ElementParser.java:815-821`), or
   ungated? Ungated silently widens the "exact-engine" surface past the engine, and
   no current gate would catch it.
2. **Does the leg survive its own numbers?** Leg 7b banks 0 rows (2 if R0 is folded
   in). Given the mission is "burn Channel B PCT to 100%", does leg 7b keep its slot,
   get re-justified as drop-in-parity work, or get deferred behind legs 1–7?
3. **Units: build or declare?** Nothing outside two legend-pure files uses unit
   literals, and the engine's own tests avoid them (`unitLibraryTests.pure:38`). Is
   there appetite for real unit semantics through a Lowerer that cannot grow? If
   not, this belongs in `docs/parser-surface-exclusions.tsv` (today header-only).
4. **What enforces the 3500-line cap?** The enforcing test was not located by this
   agent (leg 5's dossier identifies it as `CodeShapeGuardrailTest.java:35`). If the
   `SpecParser.java` risk (3440, receiving four fixes) is real, it is under-guarded.
5. **Do the 12 MODEL-WALLs belong here?** They are 63% of the wall count, all m3
   metamodel types, and unlike the parse walls they plausibly *do* cost discovery in
   other scopes. Unexamined by this dossier.
6. **`@{T[1]->U[1]}`** — `M3CoreParser.g4:467-472` permits a function type after
   `@`. Not censused; if it occurs, wall 2's fix should cover `BRACE_OPEN` too.
   **UNVERIFIED.**
7. **Behind wall 2 sits `RelationType` reflection** — `addColumns.pure:24-33` walks
   `$x.classifierGenericType.multiplicityArguments->at(0)`. Whether our model carries
   that is **UNVERIFIED**, and it may leave `addColumns.pure` a model-wall even after
   the parse fix.
8. **Do the two `PrimitiveExtension` rows actually pass after R0?** The type error
   currently masks whatever is behind it — `eq(1, 1->cast(@ExtendedInteger))` must
   then evaluate correctly under the extension's base-primitive semantics.
   **UNVERIFIED.**

---

## APPENDIX A1 — Essential lane, all 22 non-PASS rows (measured, verbatim)

| row | measured detail | leg |
|---|---|---|
| `sort::testSimpleSortWithKey` | `expected: ['Smith','Doe','Branche'] actual: ['Branche','Doe','Smith']` | 5 |
| `sort::testSimpleSortWithFunctionVariables` | same shape | 5 |
| `removeDuplicates::…MixedTypes` | `expected: [1,2,3,'1','3'] actual: ['1','3',1,2,3]` | 5 |
| `removeDuplicates::…StandardFunctionExplicit` | `ResolutionException: 'cmp_Any_1__Any_1__Boolean_1_' is not a known class, mapping, runtime, connection, or database` | 4 |
| `removeDuplicates::testRemoveDuplicatesEmptyListExplicit` | `TypeInferenceException: ambiguous overload of 'meta::pure::functions::relation::toString': 2 candidates tie` | 6 |
| `contains::testContainsWithFunction` | `ResolutionException: 'comparator_ClassWithoutEquality_1__…'` | 4 |
| `indexof::testIndexOfOneElement` | `expected: 0 actual: 1` | ledger |
| `testAdjustByMonthsBigNumber` | `Conversion Error: date field value out of range: "800002016-02-29"` | ledger |
| `testAdjustByWeeksBigNumber` | `… "236611261-10-03"` | ledger |
| `testAdjustByDaysBigNumber` | `… "33803336-12-17"` | ledger |
| `testAdjustByHoursBigNumber` | `… timestamp … "1410404-07-12T00:00:00"` | ledger |
| `match::testMatchWithFunctionsAsParam` | `no common supertype for {Integer[1] -> Integer[1]} and {String[1] -> Integer[2]}` | 3 |
| `match::…ManyMatch` | `… and {String[1] -> Integer[4]}` | 3 |
| `match::…ExtraParamsAndFunctionsAsParam` | `… {String[1],String[1] -> String[1]} and {Integer[1],String[1] -> String[1]}` | 3 |
| `match::testMatchWithMixedReturnType` | `unknown function 'deactivate'` | 3b |
| `substring::testStart` | `expected: 'he quick…' actual: 'the quick…'` | ledger |
| `substring::testStartEnd` | same shape | ledger |
| `parseDate::testParseDateTypes` | `Assert failed` | 7 |
| `indexOf::testSimple` | `expected: 4 actual: 5` | ledger |
| `indexOf::testFromIndex` | `expected: 1 actual: 2` | ledger |
| `toString::testPersonToString` | `NotImplementedException: toString over ClassType[fqn=…STR_Person] is not modeled` | 6 |
| `toString::testComplexClassToString` | `… ClassWithComplexToString …` | 6 |

**Not one of these is leg 7b.** Essential's leg-7b yield is confirmed zero from the
row list itself, independently of the `PCT.test` counting argument.

Bucket line, measured:
`AGREE-PASS=293 AGREE-FAIL=13 WIRE-BUG=9 B-FIXES-A=12 DECLINED=0`.

> **Cross-leg corroboration:** the three `match::` error texts here independently
> confirm leg 3's per-row predictions, which were derived from source without
> running anything. See `leg3.md` §b.1.

## APPENDIX A2 — Pins, exact file:line, measured value, slack

| pin | file:line | asserted | measured | slack |
|---|---|---|---|---|
| essential walls | `ChannelBEssentialTest.java:45` | `<= 20` | **19** | 1 |
| essential discovery | `ChannelBEssentialTest.java:58` | `== 327` | 327 | exact |
| essential PASS | `ChannelBEssentialTest.java:78` | `>= 305` | 305 | **0** |
| AGREE-PASS | `ChannelBEssentialTest.java:164` | `>= 293` | 293 | **0** |
| WIRE-BUG | `ChannelBEssentialTest.java:165` | `<= 9` | 9 | **0** |
| dual-verdict disagree | `ChannelBEssentialTest.java:170` | `== 0` | 0 | 0 |
| byte-verdict declines | `ChannelBEssentialTest.java:207` | `<= 0` | 0 | **0** |
| wire diverge | `ChannelBEssentialTest.java:218` | `<= 75` | — | — |
| wire adopt-pending | `ChannelBEssentialTest.java:221` | `<= 103` | — | — |
| typed-IR mismatch | `ChannelBEssentialTest.java:227` | `== 0` | 0 | 0 |
| TRUE wire-bug | `ChannelBEssentialTest.java:237` | `== 0` | 0 | 0 |
| grammar walls | `ChannelBGrammarTest.java:45` | `<= 20` | **19** | 1 |
| grammar discovery | `ChannelBGrammarTest.java:68` | `== 137` | 137 | exact |
| grammar PASS | `ChannelBGrammarTest.java:84` | `>= 133` | 133 | **0** |
| grammar WIRE-BUG | `ChannelBGrammarTest.java:85` | `<= 1` | — | — |
| grammar TRUE wire-bug | `ChannelBGrammarTest.java:87` | `== 0` | — | — |

Six pins at **zero slack**. Treat leg 7b as a change that must move *only* the two
`walls` pins and nothing else, and re-measure the full lane after each of R1–R4.

**The one pin R0 moves:** `ChannelBGrammarTest.java:84`, `pass() >= 133` → `>= 135`,
if and only if the two `PrimitiveExtension` rows flip.
