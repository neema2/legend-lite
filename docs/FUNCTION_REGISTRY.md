# Proposal: Built-in Function Registry

> **Status**: Proposal — requesting feedback  
> **Date**: 2026-03-20

## Key Insight

The Pure language already defines every function signature precisely. Instead of manually
translating these into Java types (and risking divergence), **the registry stores the actual
Pure signature strings — parsed by our existing PureParser into our structured Java model**.

```java
// The registry IS the Pure spec — copy-paste from legend-engine .pure files:
register("native function filter<T>(rel:Relation<T>[1], f:Function<{T[1]->Boolean[1]}>[1]):Relation<T>[1];");
register("native function sort<X,T>(rel:Relation<T>[1], sortInfo:SortInfo<X⊆T>[*]):Relation<T>[1];");
register("native function join<T,V>(rel1:Relation<T>[1], rel2:Relation<V>[1], joinKind:JoinKind[1], f:Function<{T[1],V[1]->Boolean[1]}>[1]):Relation<T+V>[1];");
register("native function rename<T,Z,K,V>(r:Relation<T>[1], old:ColSpec<Z=(?:K)⊆T>[1], new:ColSpec<V=(?:K)>[1]):Relation<T-Z+V>[1];");
```

**Benefits:**
- Single source of truth — the Pure string IS display text, LSP hover, NLQ catalog, AND validation spec
- Trivially verifiable — diff registry strings against legend-engine .pure files
- No translation errors — no manual mapping from Pure to Java records
- When legend-engine adds an overload, just paste the line

---

## Architecture: Unified with PureParser

The ANTLR grammar **already parses native function declarations**. The key grammar rules
(from `PureParser.g4`) are:

```antlr
nativeFunction:          NATIVE FUNCTION qualifiedName
                            typeAndMultiplicityParameters? functionTypeSignature SEMI_COLON ;

functionTypeSignature:   PAREN_OPEN (functionVariableExpression (COMMA functionVariableExpression)*)?
                            PAREN_CLOSE COLON type multiplicity ;

type:                    qualifiedName (LESS_THAN typeArguments? (PIPE multiplicityArguments)?
                            GREATER_THAN)? typeVariableValues?
                       | BRACE_OPEN functionTypePureType? (COMMA functionTypePureType)*
                            ARROW type multiplicity BRACE_CLOSE
                       | relationType | unitName ;

typeAndMultiplicityParameters:  LESS_THAN ((typeParameters multiplictyParameters?)
                                   | multiplictyParameters) GREATER_THAN ;
```

This means: **no separate parser needed**. We extend the existing `PackageableElementBuilder`
visitor to produce structured `PType` records from the `type` parse tree instead of raw strings.

```
legend-engine .pure files          BuiltinFunctionRegistry.java
         │                                      │
         │  copy-paste                          │
         ▼                                      ▼
  "native function filter<T>..."  ──→  PureParser.parseNativeFunction()
                                              │
                                  PackageableElementBuilder (enhanced)
                                              │
                                              ▼
                                     NativeFunctionDef record
                                    (PType params, Schema return,
                                     List<TypeVar>, Mult)
                                              │
                          ┌───────────────────┼───────────────────┐
                          ▼                   ▼                   ▼
                    TypeChecker          LSP hover/          NLQ function
                    validation          completions          catalog
```

### What changes in the parser

The `nativeFunction` rule already exists (L540 of `PureParser.g4`). Today it's parsed
but handled minimally by `PackageableElementBuilder`. We add:

1. **`NativeFunctionDef`** record — new model type alongside `FunctionDefinition`
2. **`PureParser.parseNativeFunction(String)`** — new entry point (like `parseClassDefinition`)
3. **Enhanced `type` visitor** — instead of flattening types to strings, produce `PType` records:
   - `Relation<T>` → `PType.Relation(Schema.Var("T"))`
   - `ColSpec<Z⊆T>` → `PType.ColSpec(Schema.Subset("Z", "T"))`
   - `Function<{T[1]->Boolean[1]}>` → `PType.Fn([PType.Var("T")], PType.P.BOOLEAN)`
   - `SortInfo<X⊆T>` → `PType.SortInfo(Schema.Subset("X", "T"))`
4. **Schema expression parsing** in `typeArguments` — recognize `T+V`, `T-Z+V`, `Any`

### What DOESN'T change

- Grammar file (`PureParser.g4`) — no changes needed
- Lexer (`PureLexer.g4`) — no changes needed
- All existing parse methods — unchanged
- All existing tests — pass as-is

> [!IMPORTANT]
> The only new code is: a `NativeFunctionDef` record, a `parseNativeFunction()` entry point,
> and enhanced `type` visiting logic in `PackageableElementBuilder` (~100 lines of visitor code).
> The Unicode `⊆` in `ColSpec<Z⊆T>` is already a valid identifier character in the lexer.

---

## Java Model (produced by enhanced visitor)

### `PType` — Pure types

```java
sealed interface PType {
    enum P implements PType {
        STRING, INTEGER, FLOAT, BOOLEAN, NUMBER, DECIMAL,
        DATE, DATE_TIME, STRICT_DATE, STRICT_TIME, ANY
    }
    record Var(String name, PType bound)         implements PType {}  // T, N:Number
    record Relation(Schema schema)               implements PType {}
    record ColSpec(Schema schema)                implements PType {}
    record ColSpecArray(Schema schema)           implements PType {}
    record SortInfo(Schema schema)               implements PType {}
    record AggColSpec(PType mapFn, PType reduceFn, Schema result)  implements PType {}
    record AggColSpecArray(PType mapFn, PType reduceFn, Schema r)  implements PType {}
    record FuncColSpec(PType fn, Schema result)   implements PType {}
    record FuncColSpecArray(PType fn, Schema result) implements PType {}
    record Window(Schema schema)                 implements PType {}  // _Window<T>
    record Fn(List<PType> inputs, PType output)  implements PType {}  // Function<{...}>
    record Enum(String name)                     implements PType {}  // JoinKind
}
```

### `Schema` — relation schema algebra

```java
sealed interface Schema {
    record Var(String name)                              implements Schema {}  // T
    record Subset(String var, String of)                 implements Schema {}  // X⊆T
    record Append(Schema left, Schema right)             implements Schema {}  // T+Z
    record Remove(Schema base, Schema sub, Schema add)   implements Schema {}  // T-Z+V
    record Computed()                                    implements Schema {}  // Any (pivot)
    record Fresh(String name)                            implements Schema {}  // R (aggregate)
}
```

| Pure syntax | Schema variant | Used by |
|-------------|---------------|---------|
| `Relation<T>` | `Var("T")` | filter, sort, limit, drop, slice, distinct, concatenate |
| `Relation<T+Z>` | `Append(Var("T"), Var("Z"))` | extend |
| `Relation<T+V>` | `Append(Var("T"), Var("V"))` | join, asOfJoin |
| `Relation<Z+R>` | `Append(Var("Z"), Var("R"))` | groupBy |
| `Relation<T-Z+V>` | `Remove(Var("T"), Var("Z"), Var("V"))` | rename |
| `Relation<R>` | `Fresh("R")` | aggregate |
| `Relation<Any>` | `Computed()` | pivot |
| `ColSpec<Z⊆T>` | `Subset("Z", "T")` | select, groupBy, sort, distinct |

### `Mult` — multiplicities (with variables)

```java
sealed interface Mult {
    record Fixed(Multiplicity value) implements Mult {}  // [1], [*], [0..1]
    record Var(String name)          implements Mult {}  // m — from sort<T|m>(col:T[m]):T[m]
}
```

---

## Audited Relation Functions (from legend-engine source)

Every signature below was extracted from
`legend-engine/.../core_functions_relation/relation/functions/`.

These are the **exact strings** that go into the registry, prefixed with
`native function` so PureParser can parse them directly.

### Shape-preserving (identity: `Relation<T> → Relation<T>`)

```
native function filter<T>(rel:Relation<T>[1], f:Function<{T[1]->Boolean[1]}>[1]):Relation<T>[1];
native function sort<X,T>(rel:Relation<T>[1], sortInfo:SortInfo<X⊆T>[*]):Relation<T>[1];
native function limit<T>(rel:Relation<T>[1], size:Integer[1]):Relation<T>[1];
native function drop<T>(rel:Relation<T>[1], size:Integer[1]):Relation<T>[1];
native function slice<T>(rel:Relation<T>[1], start:Integer[1], stop:Integer[1]):Relation<T>[1];
native function concatenate<T>(rel1:Relation<T>[1], rel2:Relation<T>[1]):Relation<T>[1];
native function size<T>(rel:Relation<T>[1]):Integer[1];
```

### Distinct (2 overloads)

```
native function distinct<T>(rel:Relation<T>[1]):Relation<T>[1];
native function distinct<X,T>(rel:Relation<T>[1], columns:ColSpecArray<X⊆T>[1]):Relation<X>[1];
```

### Select (3 overloads)

```
native function select<T>(r:Relation<T>[1]):Relation<T>[1];
native function select<T,Z>(r:Relation<T>[1], cols:ColSpec<Z⊆T>[1]):Relation<Z>[1];
native function select<T,Z>(r:Relation<T>[1], cols:ColSpecArray<Z⊆T>[1]):Relation<Z>[1];
```

### Rename

```
native function rename<T,Z,K,V>(r:Relation<T>[1], old:ColSpec<Z=(?:K)⊆T>[1], new:ColSpec<V=(?:K)>[1]):Relation<T-Z+V>[1];
```

### Extend (8 overloads)

```
# Scalar extend
native function extend<T,Z>(r:Relation<T>[1], f:FuncColSpec<{T[1]->Any[0..1]},Z>[1]):Relation<T+Z>[1];
native function extend<T,Z>(r:Relation<T>[1], fs:FuncColSpecArray<{T[1]->Any[*]},Z>[1]):Relation<T+Z>[1];

# Aggregate extend
native function extend<T,K,V,R>(r:Relation<T>[1], agg:AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<T+R>[1];
native function extend<T,K,V,R>(r:Relation<T>[1], agg:AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<T+R>[1];

# Window scalar extend
native function extend<T,Z,W,R>(r:Relation<T>[1], window:_Window<T>[1], f:FuncColSpec<{Relation<T>[1],_Window<T>[1],T[1]->Any[0..1]},R>[1]):Relation<T+R>[1];
native function extend<T,Z,W,R>(r:Relation<T>[1], window:_Window<T>[1], f:FuncColSpecArray<{Relation<T>[1],_Window<T>[1],T[1]->Any[*]},R>[1]):Relation<T+R>[1];

# Window aggregate extend
native function extend<T,K,V,R>(r:Relation<T>[1], window:_Window<T>[1], agg:AggColSpec<{Relation<T>[1],_Window<T>[1],T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<T+R>[1];
native function extend<T,K,V,R>(r:Relation<T>[1], window:_Window<T>[1], agg:AggColSpecArray<{Relation<T>[1],_Window<T>[1],T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<T+R>[1];
```

### GroupBy (4 overloads)

```
native function groupBy<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpecArray<Z⊆T>[1], agg:AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<Z+R>[1];
native function groupBy<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpec<Z⊆T>[1], agg:AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<Z+R>[1];
native function groupBy<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpecArray<Z⊆T>[1], agg:AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<Z+R>[1];
native function groupBy<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpec<Z⊆T>[1], agg:AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<Z+R>[1];
```

### Aggregate (2 overloads)

```
native function aggregate<T,K,V,R>(r:Relation<T>[1], agg:AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<R>[1];
native function aggregate<T,K,V,R>(r:Relation<T>[1], agg:AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<R>[1];
```

### Join (1) + AsOfJoin (2)

```
native function join<T,V>(rel1:Relation<T>[1], rel2:Relation<V>[1], joinKind:JoinKind[1], f:Function<{T[1],V[1]->Boolean[1]}>[1]):Relation<T+V>[1];
native function asOfJoin<T,V>(rel1:Relation<T>[1], rel2:Relation<V>[1], match:Function<{T[1],V[1]->Boolean[1]}>[1]):Relation<T+V>[1];
native function asOfJoin<T,V>(rel1:Relation<T>[1], rel2:Relation<V>[1], match:Function<{T[1],V[1]->Boolean[1]}>[1], join:Function<{T[1],V[1]->Boolean[1]}>[1]):Relation<T+V>[1];
```

### Pivot (5 overloads)

```
native function pivot<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpecArray<Z⊆T>[1], agg:AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<Any>[1];
native function pivot<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpec<Z⊆T>[1], agg:AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<Any>[1];
native function pivot<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpec<Z⊆T>[1], values:Any[1..*], agg:AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<Any>[1];
native function pivot<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpecArray<Z⊆T>[1], agg:AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<Any>[1];
native function pivot<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpec<Z⊆T>[1], agg:AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<Any>[1];
```

### Project (2 overloads)

```
# From collection to relation
native function project<C,T>(cl:C[*], x:FuncColSpecArray<{C[1]->Any[*]},T>[1]):Relation<T>[1];
# Re-project a relation
native function project<T,Z>(r:Relation<T>[1], fs:FuncColSpecArray<{T[1]->Any[*]},Z>[1]):Relation<Z>[1];
```

### Flatten

```
native function flatten<T,Z>(valueToFlatten:T[*], columnWithFlattenedValue:ColSpec<Z=(?:T)>[1]):Relation<Z>[1];
```

### Window functions (native)

```
native function first<T>(w:Relation<T>[1], f:_Window<T>[1], r:T[1]):T[0..1];
native function last<T>(w:Relation<T>[1], f:_Window<T>[1], row:T[1]):T[0..1];
native function nth<T>(w:Relation<T>[1], f:_Window<T>[1], r:T[1], offset:Integer[1]):T[0..1];
native function offset<T>(w:Relation<T>[1], r:T[1], offset:Integer[1]):T[0..1];
native function rowNumber<T>(rel:Relation<T>[1], row:T[1]):Integer[1];
native function ntile<T>(rel:Relation<T>[1], row:T[1], tileCount:Integer[1]):Integer[1];
native function rank<T>(rel:Relation<T>[1], w:_Window<T>[1], row:T[1]):Integer[1];
native function denseRank<T>(rel:Relation<T>[1], w:_Window<T>[1], row:T[1]):Integer[1];
native function percentRank<T>(rel:Relation<T>[1], w:_Window<T>[1], row:T[1]):Float[1];
native function cumulativeDistribution<T>(rel:Relation<T>[1], w:_Window<T>[1], row:T[1]):Float[1];
```

### Window functions (non-native — delegate to offset)

```
# These are regular Pure functions, not native. They delegate to native offset().
# We register them with the same signatures for completeness.
function lag<T>(w:Relation<T>[1], r:T[1]):T[0..1];
function lag<T>(w:Relation<T>[1], r:T[1], offset:Integer[1]):T[0..1];
function lead<T>(w:Relation<T>[1], r:T[1]):T[0..1];
function lead<T>(w:Relation<T>[1], r:T[1], offset:Integer[1]):T[0..1];
```

### Collection sort (from legend-pure)

```
native function sort<T,U|m>(col:T[m], key:Function<{T[1]->U[1]}>[0..1], comp:Function<{U[1],U[1]->Integer[1]}>[0..1]):T[m];
native function sort<T|m>(col:T[m]):T[m];
native function sort<T|m>(col:T[m], comp:Function<{T[1],T[1]->Integer[1]}>[0..1]):T[m];
```

### Collection functions (from legend-engine collectionExtension)

```
native function sortBy<T,U|m>(col:T[m], key:Function<{T[1]->U[1]}>[0..1]):T[m];
native function sortByReversed<T,U|m>(col:T[m], key:Function<{T[1]->U[1]}>[0..1]):T[m];
```

### Scalar functions from legend-pure (Tier 2 — representative sample)

All from `legend-pure/.../platform/pure/essential/` and `grammar/functions/`.
199 native function declarations total. Key signatures:

```
# String
native function toLower(source:String[1]):String[1];
native function toUpper(source:String[1]):String[1];
native function trim(str:String[1]):String[1];
native function ltrim(str:String[1]):String[1];
native function rtrim(str:String[1]):String[1];
native function substring(str:String[1], start:Integer[1], end:Integer[1]):String[1];
native function indexOf(str:String[1], toFind:String[1]):Integer[1];
native function indexOf(str:String[1], toFind:String[1], fromIndex:Integer[1]):Integer[1];
native function startsWith(source:String[1], val:String[1]):Boolean[1];
native function endsWith(source:String[1], val:String[1]):Boolean[1];
native function reverseString(str:String[1]):String[1];

# Math
native function abs(int:Integer[1]):Integer[1];
native function abs(float:Float[1]):Float[1];
native function abs(number:Number[1]):Number[1];
native function abs(decimal:Decimal[1]):Decimal[1];
native function ceiling(number:Number[1]):Integer[1];
native function floor(number:Number[1]):Integer[1];
native function round(number:Number[1]):Integer[1];
native function round(decimal:Decimal[1], scale:Integer[1]):Decimal[1];
native function round(float:Float[1], scale:Integer[1]):Float[1];
native function sqrt(number:Number[1]):Float[1];
native function pow(base:Number[1], exponent:Number[1]):Number[1];
native function exp(exponent:Number[1]):Float[1];
native function log(value:Number[1]):Float[1];
native function log10(value:Number[1]):Float[1];
native function sign(number:Number[1]):Integer[1];
native function sin(number:Number[1]):Float[1];
native function cos(number:Number[1]):Float[1];  # inferred from pattern
native function tan(number:Number[1]):Float[1];

# Arithmetic
native function plus(numbers:Number[*]):Number[1];
native function plus(float:Float[*]):Float[1];
native function plus(decimal:Decimal[*]):Decimal[1];
native function minus(numbers:Number[*]):Number[1];
native function minus(float:Float[*]):Float[1];
native function minus(decimal:Decimal[*]):Decimal[1];
native function times(numbers:Number[*]):Number[1];
native function times(ints:Float[*]):Float[1];
native function times(decimal:Decimal[*]):Decimal[1];

# Date
native function dateDiff(d1:Date[1], d2:Date[1], du:DurationUnit[1]):Integer[1];
native function year(d:Date[1]):Integer[1];
native function monthNumber(d:Date[1]):Integer[1];
native function hour(d:Date[1]):Integer[1];
native function minute(d:Date[1]):Integer[1];
native function second(d:Date[1]):Integer[1];

# Conversion
native function parseInteger(string:String[1]):Integer[1];
native function parseFloat(string:String[1]):Float[1];
native function parseDate(string:String[1]):Date[1];
native function parseBoolean(string:String[1]):Boolean[1];
native function toDecimal(number:Number[1]):Decimal[1];
native function toFloat(number:Number[1]):Float[1];

# Collection (with type vars + multiplicity vars)
native function toOne<T>(values:T[*], message:String[1]):T[1];
native function toOneMany<T>(values:T[*], message:String[1]):T[1..*];
native function last<T>(set:T[*]):T[0..1];
native function reverse<T|m>(set:T[m]):T[m];
native function slice<T>(set:T[*], start:Integer[1], end:Integer[1]):T[*];
native function take<T>(set:T[*], count:Integer[1]):T[*];
native function drop<T>(set:T[*], count:Integer[1]):T[*];
native function exists<T>(value:T[*], func:Function<{T[1]->Boolean[1]}>[1]):Boolean[1];
native function forAll<T>(value:T[*], func:Function<{T[1]->Boolean[1]}>[1]):Boolean[1];
native function find<T>(value:T[*], func:Function<{T[1]->Boolean[1]}>[1]):T[0..1];
native function map<T,V>(value:T[*], func:Function<{T[1]->V[*]}>[1]):V[*];
native function map<T,V>(value:T[0..1], func:Function<{T[1]->V[0..1]}>[1]):V[0..1];
native function zip<T,U>(set1:T[*], set2:U[*]):Pair<T,U>[*];
native function indexOf<T>(set:T[*], value:T[1]):Integer[1];
native function removeAllOptimized<T>(set:T[*], other:T[*]):T[*];

# Boolean
native function instanceOf(instance:Any[1], type:Type[1]):Boolean[1];
```

---

## Testing Strategy

Every Pure signature in the registry MUST have a corresponding test that:

1. **Parses the signature** via `PureParser.parseNativeFunction()`
2. **Asserts the exact Java model** — every field of the generated `NativeFunctionDef`

```java
@Test void testFilterSignature() {
    var fn = PureParser.parseNativeFunction(
        "native function filter<T>(rel:Relation<T>[1], " +
        "f:Function<{T[1]->Boolean[1]}>[1]):Relation<T>[1];"
    );
    assertEquals("filter", fn.name());
    assertEquals(2, fn.params().size());

    // param 0: rel:Relation<T>[1]
    assertInstanceOf(PType.Relation.class, fn.params().get(0).type());
    var relSchema = ((PType.Relation) fn.params().get(0).type()).schema();
    assertInstanceOf(Schema.Var.class, relSchema);
    assertEquals("T", ((Schema.Var) relSchema).name());

    // param 1: f:Function<{T[1]->Boolean[1]}>[1]
    assertInstanceOf(PType.Fn.class, fn.params().get(1).type());

    // return: Relation<T>[1] — same T as input → identity
    assertInstanceOf(PType.Relation.class, fn.returnType());
    assertEquals("T", ((Schema.Var) ((PType.Relation) fn.returnType()).schema()).name());
}

@Test void testJoinSignature() {
    var fn = PureParser.parseNativeFunction(
        "native function join<T,V>(rel1:Relation<T>[1], rel2:Relation<V>[1], " +
        "joinKind:JoinKind[1], f:Function<{T[1],V[1]->Boolean[1]}>[1]):Relation<T+V>[1];"
    );
    assertEquals("join", fn.name());
    assertEquals(4, fn.params().size());

    // return: Relation<T+V> — schema append
    var retSchema = ((PType.Relation) fn.returnType()).schema();
    assertInstanceOf(Schema.Append.class, retSchema);
    assertEquals("T", ((Schema.Var) ((Schema.Append) retSchema).left()).name());
    assertEquals("V", ((Schema.Var) ((Schema.Append) retSchema).right()).name());
}

@Test void testRenameSignature() {
    var fn = PureParser.parseNativeFunction(
        "native function rename<T,Z,K,V>(r:Relation<T>[1], " +
        "old:ColSpec<Z=(?:K)⊆T>[1], new:ColSpec<V=(?:K)>[1]):Relation<T-Z+V>[1];"
    );
    var retSchema = ((PType.Relation) fn.returnType()).schema();
    assertInstanceOf(Schema.Remove.class, retSchema);
}

@Test void testSortWithMultiplicityVar() {
    var fn = PureParser.parseNativeFunction(
        "native function sort<T|m>(col:T[m]):T[m];"
    );
    assertInstanceOf(Mult.Var.class, fn.params().get(0).multiplicity());
    assertInstanceOf(Mult.Var.class, fn.returnMultiplicity());
    assertEquals("m", ((Mult.Var) fn.returnMultiplicity()).name());
}

@Test void testAllRelationFunctionsParse() {
    // Load all signatures from BuiltinFunctionRegistry
    // Assert each one parses without error and produces valid PType records
    for (String sig : BuiltinFunctionRegistry.ALL_SIGNATURES) {
        var fn = PureParser.parseNativeFunction(sig);
        assertNotNull(fn.name(), "Failed to parse: " + sig);
        assertNotNull(fn.returnType(), "No return type for: " + sig);
    }
}
```

---

## Two Tiers of Functions

The registry has **one unified store** but functions fall into two tiers with different
characteristics:

### Tier 1: Relation Functions (~30 functions, ~40 overloads)

These transform data **shape** — they manipulate relation schemas.

| Characteristic | Detail |
|----------------|--------|
| **Signatures** | Rich type vars: `Relation<T+V>`, `ColSpec<Z⊆T>`, `AggColSpec<...>` |
| **Source of truth** | `native function` declarations in legend-engine `.pure` files |
| **TypeChecker** | Custom `compile*` methods (schema manipulation) |
| **SQL layer** | Structural: SELECT, JOIN, GROUP BY, WINDOW clauses |
| **Parsing** | Full `PureParser.parseNativeFunction()` with structured `PType` |

### Tier 2: DynaFunctions (~225 functions)

These transform data **values** — scalars, aggregates, window functions.

| Characteristic | Detail |
|----------------|--------|
| **Signatures** | Simple: primitives in → primitives out (with some type propagation) |
| **Source of truth** | `DynaFunctionRegistry` enum in legend-engine `dbExtension.pure` + `native function` declarations in legend-pure `essential/` |
| **TypeChecker** | Generic: `compileTypePropagating` / `knownReturnType` patterns |
| **SQL layer** | `dynaFnToSql` dispatch → format strings per database dialect |
| **Parsing** | Simpler — many are just `name(Type[1]): Type[1]` |

---

## DynaFunctionRegistry — Complete Catalog (from legend-engine)

Source: `legend-engine/.../sqlQueryToString/dbExtension.pure` L1078-1304.

Every function name below has a `dynaFnToSql` dispatch in each database extension.
These are the **exact names** used in `DynaFunction.name`.

### String (~25)
```
toLower, toUpper, trim, ltrim, rtrim, length, substring, contains, startsWith,
endsWith, replace, indexOf, concat, lpad, rpad, reverseString, splitPart,
repeatString, ascii, char, left, right, position, matches
```

### Math (~25)
```
abs, ceiling, floor, round, sqrt, cbrt, exp, log, log10, pow, mod, rem, sign,
sin, cos, tan, asin, acos, atan, atan2, sinh, cosh, tanh, cot
```

### Arithmetic (7)
```
plus, minus, times, divide, divideRound, add, sub
```

### Comparison (10)
```
equal, notEqual, notEqualAnsi, greaterThan, lessThan, greaterThanEqual,
lessThanEqual, in, between, greatest, least
```

### Boolean (10)
```
and, or, not, isEmpty, isNotEmpty, isNull, isNotNull, isNumeric,
isAlphaNumeric, exists, isDistinct, booland, boolor
```

### Date/Time (~30)
```
now, today, year, month, monthNumber, monthName, dayOfMonth, dayOfWeek,
dayOfWeekNumber, dayOfYear, hour, minute, second, quarter, quarterNumber,
weekOfYear, dateDiff, datePart, date, adjust, convertDate, convertDateTime,
convertTimeZone, toTimestamp, firstDayOfMonth, firstDayOfQuarter,
firstDayOfYear, firstDayOfWeek, firstDayOfThisMonth, firstDayOfThisQuarter,
firstDayOfThisYear, firstHourOfDay, firstMinuteOfHour, firstSecondOfMinute,
firstMillisecondOfSecond, mostRecentDayOfWeek, previousDayOfWeek, timeBucket
```

### Aggregate (~18)
```
count, sum, average, min, max, median, stdDevSample, stdDevPopulation,
variance, varianceSample, variancePopulation, corr, covarSample,
covarPopulation, percentile, mode, hashAgg, joinStrings
```

### Window (12)
```
rowNumber, rank, denseRank, percentRank, cumulativeDistribution,
lag, lead, first, last, nth, ntile, averageRank
```

### Conversion (11)
```
parseInteger, parseFloat, parseDecimal, parseDate, parseBoolean, parseJson,
toDecimal, toFloat, toString, cast, castBoolean
```

### Hash/Encode (6)
```
md5, sha1, sha256, encodeBase64, decodeBase64, hashCode
```

### Bitwise (6)
```
bitAnd, bitOr, bitXor, bitNot, bitShiftLeft, bitShiftRight
```

### Variant/Semi-structured (10)
```
toVariant, toVariantList, toVariantObject, variantTo, parseJson, toJson,
explodeSemiStructured, extractFromSemiStructured, keys, values
```

### Array (18)
```
array_first, array_last, array_init, array_tail, array_concatenate,
array_max, array_min, array_sum, array_sort, array_reverse, array_size,
array_append, array_position, array_slice, array_drop, array_take,
array_contains, array_distinct, array_flatten
```

### Control Flow / Other (~10)
```
coalesce, if, case, generateGuid, currentUserId, distinct, size,
toOne, range, pair, regexpLike, regexpCount, regexpExtract, regexpIndexOf,
regexpReplace, jaroWinklerSimilarity, levenshteinDistance, convertVarchar128,
sqlNull, sqlTrue, sqlFalse, group, objectReferenceIn, mapConcatenate,
maxBy, minBy
```

---

## How TypeChecker uses this

```java
default -> {
    var sig = registry.resolve(funcName, af.parameters(), compiledTypes);
    if (sig == null) {
        // NO MORE PASSTHROUGH — every function must be registered
        throw new PureCompileException("Unknown function: " + funcName);
    }
    sig.validateParams(af.parameters(), compiledTypes);
    yield compileFromSignature(af, sig, ctx);
}
```

> [!IMPORTANT]
> No passthrough. If a function isn't registered, it's a compile error.
> This is the enforcement mechanism that guarantees complete type coverage.

---

## Implementation Plan

### Phase 1: Core types + PureParser enhancement
1. Create `PType`, `Schema`, `Mult` sealed interfaces
2. Create `NativeFunctionDef` record (alongside existing `FunctionDefinition`)
3. Add `PureParser.parseNativeFunction()` entry point
4. Enhance `PackageableElementBuilder` to produce `PType` from `type` parse nodes (~100 lines)

### Phase 2: Relation functions (Tier 1)
5. Register all ~40 relation function overloads (Pure signature strings)
6. Write exhaustive parse tests asserting exact Java model for every signature
7. Wire into TypeChecker: structural pre-validation from registry
8. Custom `compile*` methods stay — but registry drives arity/shape checks

### Phase 3: DynaFunctions (Tier 2) — eliminate passthrough
9. Register all ~225 DynaFunctions with typed signatures
10. Replace `knownReturnType()`, `isWindowAggregate()` with registry lookups
11. Replace the 65-line type-propagating case block with registry-driven compilation
12. **Delete `compilePassThrough`** — unknown function = compile error
13. All existing tests pass unchanged

### Phase 4: Hardening
14. Audit legend-lite TypeChecker switch cases against registry — zero gaps
15. Add overload resolution for multi-signature functions
16. Verify every DynaFunctionRegistry enum value has a registry entry

### Phase 5: Consumers (deferred)
17. LSP hover → display the raw Pure signature string
18. LSP completions → `registry.all()`
19. NLQ catalog → typed function list for LLM prompt grounding
