# Proposal: Built-in Function Registry

> **Status**: Proposal — requesting feedback  
> **Date**: 2026-03-19

## Motivation

All function type knowledge in Legend Lite is **implicit in Java code**, scattered across three locations with no single source of truth:

| Location | What it knows |
|----------|---------------|
| `TypeChecker.compileFunction()` switch | Which functions exist (~100 cases) |
| `TypeChecker.knownReturnType()` | Return types for ~40 functions |
| `PlanGenerator.generateScalarFunction()` | Pure→SQL mapping for ~90 functions |

This means:
- **No arity validation** — `toLower(x, y)` silently ignores `y`
- **No input type checking** — `'hello'->abs()` fails at SQL time, not compile time
- **No LSP support** — no completions, no hover, no function discovery
- **No NLQ catalog** — the LLM guesses function names
- **No self-documenting spec** — you have to read Java source to learn the language

---

## Core Model: `PureType`

A single sealed interface models **both** parameter types and return types. Each variant captures what a Pure value *is* at the type system level — not what Java AST class it happens to be.

```java
sealed interface PureType {

    // === Value types (params + returns) ===
    record Scalar(GenericType.Primitive type) implements PureType {}
    record Propagated()                      implements PureType {}

    // === Structural types (params — distinctive syntactic shapes) ===
    record Lambda(PureType returnType)       implements PureType {}
    record ColSpec()                         implements PureType {}
    record ColSpecArray()                    implements PureType {}
    record GraphFetchTree()                  implements PureType {}
    record JoinKind()                        implements PureType {}
    record Collection()                      implements PureType {}

    // === Relation types (params + returns, schema flow embedded) ===
    record Relation()                        implements PureType {}
    record RelationIdentity()                implements PureType {}
    record RelationSubset()                  implements PureType {}
    record RelationAppend()                  implements PureType {}
    record RelationUnion(int left, int right) implements PureType {}
    record RelationComputed()                implements PureType {}

    // === Wildcard ===
    record Any()                             implements PureType {}
}
```

**Why a unified type?**

- **Composable** — `new Lambda(new Scalar(P.BOOLEAN))` reads as "a lambda returning Boolean"
- **No nulls** — every param and return has a real `PureType` value
- **One vocabulary** — params and returns use the same type family
- **Return type IS the schema flow** — `RelationIdentity` vs `RelationComputed` collapses what were two separate fields
- **Pattern-matchable** — Java 21 exhaustive `switch` on sealed types

### What each variant means

| Variant | As parameter | As return type |
|---------|-------------|----------------|
| `Scalar(P.STRING)` | must be a String value | returns String |
| `Scalar(P.NUMBER)` | must be numeric | returns numeric |
| `Propagated()` | — | same type as input |
| `Lambda(returnType)` | `{x \| ...}` with body returning `returnType` | — |
| `ColSpec()` | `~col` or `~col:x\|$x.prop` | — |
| `ColSpecArray()` | `~[col1, col2]` | — |
| `GraphFetchTree()` | `#{ Class { props } }#` | — |
| `JoinKind()` | `JoinKind.INNER` etc. | — |
| `Collection()` | `['a', 'b']` | — |
| `Relation()` | any expression resolving to a Relation | — |
| `RelationIdentity()` | — | Relation with same schema as input |
| `RelationSubset()` | — | Relation with subset of input columns |
| `RelationAppend()` | — | Relation with input columns + new columns |
| `RelationUnion(0, 2)` | — | Relation with columns from params 0 and 2 |
| `RelationComputed()` | — | Relation with schema computed by compile method |
| `Any()` | no constraint | no constraint |

---

## Function Records

```java
record Param(String name, PureType type, Multiplicity multiplicity) {}

record BuiltinFunction(
    String name,
    Category category,           // SCALAR, AGGREGATE, WINDOW, RELATION
    String pureSignature,        // display text for LSP / docs
    List<Param> params,
    PureType returnType,
    String sqlHint               // optional SQL function name
) {
    enum Category { SCALAR, AGGREGATE, WINDOW, RELATION }
}
```

Param `multiplicity` uses the existing `com.gs.legend.model.m3.Multiplicity` record: `ONE`, `ZERO_ONE`, `MANY`, `ONE_MANY`.

---

## Validation

Each `PureType` variant maps to a specific check — some are AST-level (syntactic shape), some are semantic (compiled type):

```java
boolean matches(PureType constraint, ValueSpecification vs, TypeInfo compiled) {
    return switch (constraint) {
        case Scalar(var t)     -> compiled != null && compiled.scalarType() != null
                                  && t.isSubtypeOf(compiled.scalarType().asPrimitive());
        case Relation()        -> compiled != null && compiled.relationType() != null;
        case Lambda(var ret)   -> vs instanceof LambdaFunction;
        case ColSpec()         -> vs instanceof ClassInstance ci && ci.value() instanceof ColSpec;
        case ColSpecArray()    -> vs instanceof ClassInstance ci && ci.value() instanceof ColSpecArray;
        case GraphFetchTree()  -> vs instanceof ClassInstance ci && ci.value() instanceof GraphFetchTree;
        case JoinKind()        -> vs instanceof EnumValue;
        case Collection()      -> vs instanceof PureCollection;
        case Any()             -> true;
        default                -> true;
    };
}
```

---

## Complete Function Catalogue

Shorthand: `P.STRING` = `GenericType.Primitive.STRING`, `ONE` = `Multiplicity.ONE`, etc.

### Scalar functions

```java
// --- String ---
fn("toLower",    SCALAR,  "toLower(s: String[1]): String[1]",
   params(param("s", new Scalar(P.STRING), ONE)),
   new Scalar(P.STRING), "LOWER");

fn("toUpper",    SCALAR,  "toUpper(s: String[1]): String[1]",
   params(param("s", new Scalar(P.STRING), ONE)),
   new Scalar(P.STRING), "UPPER");

fn("trim",       SCALAR,  "trim(s: String[1]): String[1]",
   params(param("s", new Scalar(P.STRING), ONE)),
   new Scalar(P.STRING), "TRIM");

fn("length",     SCALAR,  "length(s: String[1]): Integer[1]",
   params(param("s", new Scalar(P.STRING), ONE)),
   new Scalar(P.INTEGER), "LENGTH");

fn("substring",  SCALAR,  "substring(s: String[1], start: Integer[1], end: Integer[0..1]): String[1]",
   params(param("s",     new Scalar(P.STRING),  ONE),
          param("start", new Scalar(P.INTEGER), ONE),
          param("end",   new Scalar(P.INTEGER), ZERO_ONE)),
   new Scalar(P.STRING), "SUBSTRING");

fn("contains",   SCALAR,  "contains(s: String[1], sub: String[1]): Boolean[1]",
   params(param("s",   new Scalar(P.STRING), ONE),
          param("sub", new Scalar(P.STRING), ONE)),
   new Scalar(P.BOOLEAN), null);

fn("startsWith", SCALAR,  "startsWith(s: String[1], prefix: String[1]): Boolean[1]",
   params(param("s",      new Scalar(P.STRING), ONE),
          param("prefix", new Scalar(P.STRING), ONE)),
   new Scalar(P.BOOLEAN), null);

fn("replace",    SCALAR,  "replace(s: String[1], target: String[1], repl: String[1]): String[1]",
   params(param("s",      new Scalar(P.STRING), ONE),
          param("target", new Scalar(P.STRING), ONE),
          param("repl",   new Scalar(P.STRING), ONE)),
   new Scalar(P.STRING), "REPLACE");

fn("indexOf",    SCALAR,  "indexOf(s: String[1], sub: String[1], from: Integer[0..1]): Integer[1]",
   params(param("s",    new Scalar(P.STRING),  ONE),
          param("sub",  new Scalar(P.STRING),  ONE),
          param("from", new Scalar(P.INTEGER), ZERO_ONE)),
   new Scalar(P.INTEGER), null);

fn("toString",   SCALAR,  "toString(val: Any[1]): String[1]",
   params(param("val", new Any(), ONE)),
   new Scalar(P.STRING), null);

fn("format",     SCALAR,  "format(val: Any[1], fmt: String[0..1]): String[1]",
   params(param("val", new Any(), ONE),
          param("fmt", new Scalar(P.STRING), ZERO_ONE)),
   new Scalar(P.STRING), null);

fn("split",      SCALAR,  "split(s: String[1], delim: String[1]): String[*]",
   params(param("s",     new Scalar(P.STRING), ONE),
          param("delim", new Scalar(P.STRING), ONE)),
   new Scalar(P.STRING), null);       // returns list, propagated

fn("joinStrings", SCALAR, "joinStrings(strs: String[*], sep: String[0..1]): String[1]",
   params(param("strs", new Scalar(P.STRING), MANY),
          param("sep",  new Scalar(P.STRING), ZERO_ONE)),
   new Scalar(P.STRING), null);

// --- Math ---
fn("abs",        SCALAR,  "abs(val: Number[1]): Number[1]",
   params(param("val", new Scalar(P.NUMBER), ONE)),
   new Propagated(), "ABS");

fn("ceiling",    SCALAR,  "ceiling(val: Number[1]): Integer[1]",
   params(param("val", new Scalar(P.NUMBER), ONE)),
   new Scalar(P.INTEGER), "CEIL");

fn("floor",      SCALAR,  "floor(val: Number[1]): Integer[1]",
   params(param("val", new Scalar(P.NUMBER), ONE)),
   new Scalar(P.INTEGER), "FLOOR");

fn("round",      SCALAR,  "round(val: Number[1], scale: Integer[0..1]): Integer[1]",
   params(param("val",   new Scalar(P.NUMBER),  ONE),
          param("scale", new Scalar(P.INTEGER), ZERO_ONE)),
   new Scalar(P.INTEGER), "ROUND");

fn("sqrt",       SCALAR,  "sqrt(val: Number[1]): Float[1]",
   params(param("val", new Scalar(P.NUMBER), ONE)),
   new Scalar(P.FLOAT), "SQRT");

fn("pow",        SCALAR,  "pow(base: Number[1], exp: Number[1]): Number[1]",
   params(param("base", new Scalar(P.NUMBER), ONE),
          param("exp",  new Scalar(P.NUMBER), ONE)),
   new Propagated(), "POWER");

fn("exp",        SCALAR,  "exp(val: Number[1]): Float[1]",
   params(param("val", new Scalar(P.NUMBER), ONE)),
   new Scalar(P.FLOAT), "EXP");

fn("log",        SCALAR,  "log(val: Number[1]): Float[1]",
   params(param("val", new Scalar(P.NUMBER), ONE)),
   new Scalar(P.FLOAT), "LN");

fn("mod",        SCALAR,  "mod(a: Integer[1], b: Integer[1]): Integer[1]",
   params(param("a", new Scalar(P.INTEGER), ONE),
          param("b", new Scalar(P.INTEGER), ONE)),
   new Scalar(P.INTEGER), "MOD");

fn("sign",       SCALAR,  "sign(val: Number[1]): Integer[1]",
   params(param("val", new Scalar(P.NUMBER), ONE)),
   new Scalar(P.INTEGER), "SIGN");

fn("plus",       SCALAR,  "plus(a: Any[1], b: Any[1]): Any[1]",
   params(param("a", new Any(), ONE),
          param("b", new Any(), ONE)),
   new Propagated(), null);            // polymorphic: + or ||

fn("minus",      SCALAR,  "minus(a: Number[1], b: Number[0..1]): Number[1]",
   params(param("a", new Scalar(P.NUMBER), ONE),
          param("b", new Scalar(P.NUMBER), ZERO_ONE)),
   new Propagated(), null);

fn("times",      SCALAR,  "times(a: Number[1], b: Number[1]): Number[1]",
   params(param("a", new Scalar(P.NUMBER), ONE),
          param("b", new Scalar(P.NUMBER), ONE)),
   new Propagated(), null);

fn("divide",     SCALAR,  "divide(a: Number[1], b: Number[1], scale: Integer[0..1]): Number[1]",
   params(param("a",     new Scalar(P.NUMBER),  ONE),
          param("b",     new Scalar(P.NUMBER),  ONE),
          param("scale", new Scalar(P.INTEGER), ZERO_ONE)),
   new Propagated(), null);

// --- Boolean / Comparison ---
fn("not",        SCALAR,  "not(val: Boolean[1]): Boolean[1]",
   params(param("val", new Scalar(P.BOOLEAN), ONE)),
   new Scalar(P.BOOLEAN), null);

fn("and",        SCALAR,  "and(a: Boolean[1], b: Boolean[1]): Boolean[1]",
   params(param("a", new Scalar(P.BOOLEAN), ONE),
          param("b", new Scalar(P.BOOLEAN), ONE)),
   new Scalar(P.BOOLEAN), null);

fn("or",         SCALAR,  "or(a: Boolean[1], b: Boolean[1]): Boolean[1]",
   params(param("a", new Scalar(P.BOOLEAN), ONE),
          param("b", new Scalar(P.BOOLEAN), ONE)),
   new Scalar(P.BOOLEAN), null);

fn("isEmpty",    SCALAR,  "isEmpty(val: Any[*]): Boolean[1]",
   params(param("val", new Any(), MANY)),
   new Scalar(P.BOOLEAN), null);

fn("isNotEmpty",  SCALAR, "isNotEmpty(val: Any[*]): Boolean[1]",
   params(param("val", new Any(), MANY)),
   new Scalar(P.BOOLEAN), null);

fn("in",         SCALAR,  "in(val: Any[1], coll: Any[*]): Boolean[1]",
   params(param("val",  new Any(), ONE),
          param("coll", new Collection(), MANY)),
   new Scalar(P.BOOLEAN), null);

fn("equal",      SCALAR,  "equal(a: Any[1], b: Any[1]): Boolean[1]",
   params(param("a", new Any(), ONE), param("b", new Any(), ONE)),
   new Scalar(P.BOOLEAN), null);

fn("greaterThan", SCALAR, "greaterThan(a: Any[1], b: Any[1]): Boolean[1]",
   params(param("a", new Any(), ONE), param("b", new Any(), ONE)),
   new Scalar(P.BOOLEAN), null);

fn("lessThan",   SCALAR,  "lessThan(a: Any[1], b: Any[1]): Boolean[1]",
   params(param("a", new Any(), ONE), param("b", new Any(), ONE)),
   new Scalar(P.BOOLEAN), null);

// --- Date/Time ---
fn("now",        SCALAR,  "now(): DateTime[1]",
   params(),
   new Scalar(P.DATE_TIME), null);

fn("today",      SCALAR,  "today(): StrictDate[1]",
   params(),
   new Scalar(P.STRICT_DATE), null);

fn("year",       SCALAR,  "year(dt: Date[1]): Integer[1]",
   params(param("dt", new Scalar(P.DATE), ONE)),
   new Scalar(P.INTEGER), null);

fn("monthNumber", SCALAR, "monthNumber(dt: Date[1]): Integer[1]",
   params(param("dt", new Scalar(P.DATE), ONE)),
   new Scalar(P.INTEGER), null);

fn("dayOfMonth", SCALAR,  "dayOfMonth(dt: Date[1]): Integer[1]",
   params(param("dt", new Scalar(P.DATE), ONE)),
   new Scalar(P.INTEGER), null);

fn("dateDiff",   SCALAR,  "dateDiff(d1: Date[1], d2: Date[1], unit: DurationUnit[1]): Integer[1]",
   params(param("d1",   new Scalar(P.DATE), ONE),
          param("d2",   new Scalar(P.DATE), ONE),
          param("unit", new Any(),          ONE)),
   new Scalar(P.INTEGER), null);

// --- Conversion ---
fn("parseInteger", SCALAR, "parseInteger(s: String[1]): Integer[1]",
   params(param("s", new Scalar(P.STRING), ONE)),
   new Scalar(P.INTEGER), null);

fn("parseFloat",  SCALAR,  "parseFloat(s: String[1]): Float[1]",
   params(param("s", new Scalar(P.STRING), ONE)),
   new Scalar(P.FLOAT), null);

fn("parseBoolean", SCALAR, "parseBoolean(s: String[1]): Boolean[1]",
   params(param("s", new Scalar(P.STRING), ONE)),
   new Scalar(P.BOOLEAN), null);

fn("toDecimal",   SCALAR,  "toDecimal(val: Number[1]): Decimal[1]",
   params(param("val", new Scalar(P.NUMBER), ONE)),
   new Scalar(P.DECIMAL), null);
```

### Aggregate functions

```java
fn("count",      AGGREGATE, "count(col: Any[*]): Integer[1]",
   params(param("col", new Any(), MANY)),
   new Scalar(P.INTEGER), "COUNT");

fn("sum",        AGGREGATE, "sum(col: Number[*]): Number[1]",
   params(param("col", new Scalar(P.NUMBER), MANY)),
   new Propagated(), "SUM");

fn("average",    AGGREGATE, "average(col: Number[*]): Float[1]",
   params(param("col", new Scalar(P.NUMBER), MANY)),
   new Scalar(P.FLOAT), "AVG");

fn("min",        AGGREGATE, "min(col: Any[*]): Any[1]",
   params(param("col", new Any(), MANY)),
   new Propagated(), "MIN");

fn("max",        AGGREGATE, "max(col: Any[*]): Any[1]",
   params(param("col", new Any(), MANY)),
   new Propagated(), "MAX");

fn("median",     AGGREGATE, "median(col: Number[*]): Float[1]",
   params(param("col", new Scalar(P.NUMBER), MANY)),
   new Scalar(P.FLOAT), "MEDIAN");

fn("stdDevSample", AGGREGATE, "stdDevSample(col: Number[*]): Float[1]",
   params(param("col", new Scalar(P.NUMBER), MANY)),
   new Scalar(P.FLOAT), "STDDEV_SAMP");

fn("variance",   AGGREGATE, "variance(col: Number[*]): Float[1]",
   params(param("col", new Scalar(P.NUMBER), MANY)),
   new Scalar(P.FLOAT), "STDDEV");

fn("corr",       AGGREGATE, "corr(x: Number[*], y: Number[*]): Float[1]",
   params(param("x", new Scalar(P.NUMBER), MANY),
          param("y", new Scalar(P.NUMBER), MANY)),
   new Scalar(P.FLOAT), "CORR");

fn("percentileCont", AGGREGATE, "percentileCont(col: Number[*], pct: Float[1]): Float[1]",
   params(param("col", new Scalar(P.NUMBER), MANY),
          param("pct", new Scalar(P.FLOAT),  ONE)),
   new Scalar(P.FLOAT), null);

fn("joinStrings", AGGREGATE, "joinStrings(col: String[*], sep: String[0..1]): String[1]",
   params(param("col", new Scalar(P.STRING), MANY),
          param("sep", new Scalar(P.STRING), ZERO_ONE)),
   new Scalar(P.STRING), null);
```

### Window functions

```java
fn("rowNumber",  WINDOW,  "rowNumber(window: Window[1]): Integer[1]",
   params(param("window", new Any(), ONE)),
   new Scalar(P.INTEGER), "ROW_NUMBER");

fn("rank",       WINDOW,  "rank(window: Window[1]): Integer[1]",
   params(param("window", new Any(), ONE)),
   new Scalar(P.INTEGER), "RANK");

fn("denseRank",  WINDOW,  "denseRank(window: Window[1]): Integer[1]",
   params(param("window", new Any(), ONE)),
   new Scalar(P.INTEGER), "DENSE_RANK");

fn("lag",        WINDOW,  "lag(col: Any[1], offset: Integer[0..1], default: Any[0..1]): Any[1]",
   params(param("col",     new Any(),              ONE),
          param("offset",  new Scalar(P.INTEGER),  ZERO_ONE),
          param("default", new Any(),              ZERO_ONE)),
   new Propagated(), "LAG");

fn("lead",       WINDOW,  "lead(col: Any[1], offset: Integer[0..1], default: Any[0..1]): Any[1]",
   params(param("col",     new Any(),              ONE),
          param("offset",  new Scalar(P.INTEGER),  ZERO_ONE),
          param("default", new Any(),              ZERO_ONE)),
   new Propagated(), "LEAD");

fn("percentRank", WINDOW, "percentRank(window: Window[1]): Float[1]",
   params(param("window", new Any(), ONE)),
   new Scalar(P.FLOAT), "PERCENT_RANK");
```

### Shape-changing (Relation) functions

```java
fn("filter",      RELATION, "filter<T>(rel: Relation<T>[1], fn: {T[1]->Boolean[1]}[1]): Relation<T>[1]",
   params(param("rel", new Relation(),                          ONE),
          param("fn",  new Lambda(new Scalar(P.BOOLEAN)),       ONE)),
   new RelationIdentity(), null);

fn("sort",        RELATION, "sort<T>(rel: Relation<T>[1], specs: ColSpec<T>[*]): Relation<T>[1]",
   params(param("rel",   new Relation(), ONE),
          param("specs", new ColSpec(),  MANY)),
   new RelationIdentity(), null);

fn("limit",       RELATION, "limit<T>(rel: Relation<T>[1], n: Integer[1]): Relation<T>[1]",
   params(param("rel", new Relation(),          ONE),
          param("n",   new Scalar(P.INTEGER),   ONE)),
   new RelationIdentity(), null);

fn("take",        RELATION, "take<T>(rel: Relation<T>[1], n: Integer[1]): Relation<T>[1]",
   params(param("rel", new Relation(),          ONE),
          param("n",   new Scalar(P.INTEGER),   ONE)),
   new RelationIdentity(), null);

fn("drop",        RELATION, "drop<T>(rel: Relation<T>[1], n: Integer[1]): Relation<T>[1]",
   params(param("rel", new Relation(),          ONE),
          param("n",   new Scalar(P.INTEGER),   ONE)),
   new RelationIdentity(), null);

fn("distinct",    RELATION, "distinct<T>(rel: Relation<T>[1]): Relation<T>[1]",
   params(param("rel", new Relation(), ONE)),
   new RelationIdentity(), null);

fn("select",      RELATION, "select<T>(rel: Relation<T>[1], cols: ColSpec<T>[*]): Relation<subset(T)>[1]",
   params(param("rel",  new Relation(), ONE),
          param("cols", new ColSpec(),  MANY)),
   new RelationSubset(), null);

fn("extend",      RELATION, "extend<T>(rel: Relation<T>[1], newCols: ColSpec<T>[*]): Relation<T ∪ newCols>[1]",
   params(param("rel",     new Relation(), ONE),
          param("newCols", new ColSpec(),  MANY)),
   new RelationAppend(), null);

fn("project",     RELATION, "project<T>(rel: Relation<T>[1], cols: ColSpec<T>[*]): Relation<Any>[1]",
   params(param("rel",  new Relation(), ONE),
          param("cols", new ColSpec(),  MANY)),
   new RelationComputed(), null);

fn("groupBy",     RELATION, "groupBy<T>(rel: Relation<T>[1], groups: ColSpecArray<T>[1], aggs: ColSpecArray<T>[1]): Relation<Any>[1]",
   params(param("rel",    new Relation(),     ONE),
          param("groups", new ColSpecArray(), ONE),
          param("aggs",   new ColSpecArray(), ONE)),
   new RelationComputed(), null);

fn("rename",      RELATION, "rename<T>(rel: Relation<T>[1], old: ColSpec<T>[1], new: ColSpec<?>[1]): Relation<Any>[1]",
   params(param("rel", new Relation(), ONE),
          param("old", new ColSpec(),  ONE),
          param("new", new ColSpec(),  ONE)),
   new RelationComputed(), null);

fn("join",        RELATION, "join<T,U>(left: Relation<T>[1], kind: JoinKind[1], right: Relation<U>[1], fn: {T[1],U[1]->Boolean[1]}[1]): Relation<T ∪ U>[1]",
   params(param("left",  new Relation(),                       ONE),
          param("kind",  new JoinKind(),                       ONE),
          param("right", new Relation(),                       ONE),
          param("fn",    new Lambda(new Scalar(P.BOOLEAN)),    ONE)),
   new RelationUnion(0, 2), null);

fn("concatenate", RELATION, "concatenate<T>(left: Relation<T>[1], right: Relation<T>[1]): Relation<T>[1]",
   params(param("left",  new Relation(), ONE),
          param("right", new Relation(), ONE)),
   new RelationUnion(0, 1), null);

fn("pivot",       RELATION, "pivot<T>(rel: Relation<T>[1], pivotCols: ColSpecArray<T>[1], aggs: ColSpecArray<T>[1]): Relation<Any>[1]",
   params(param("rel",       new Relation(),     ONE),
          param("pivotCols", new ColSpecArray(), ONE),
          param("aggs",      new ColSpecArray(), ONE)),
   new RelationComputed(), null);

fn("flatten",     RELATION, "flatten<T>(rel: Relation<T>[1], col: ColSpec<T>[1]): Relation<Any>[1]",
   params(param("rel", new Relation(), ONE),
          param("col", new ColSpec(),  ONE)),
   new RelationComputed(), null);

fn("graphFetch",  RELATION, "graphFetch<T>(src: T[*], tree: GraphFetchTree<T>[1]): T[*]",
   params(param("src",  new Relation(),       MANY),
          param("tree", new GraphFetchTree(), ONE)),
   new RelationComputed(), null);

fn("serialize",   RELATION, "serialize<T>(src: T[*], tree: GraphFetchTree<T>[1]): String[1]",
   params(param("src",  new Relation(),       MANY),
          param("tree", new GraphFetchTree(), ONE)),
   new Scalar(P.STRING), null);
```

---

## Two-Phase Validation

### Phase 1: Structural (registry)

```java
var sig = registry.resolve(funcName, argCount);
if (sig.isPresent()) {
    sig.get().validateStructure(af.parameters());
    // error: "filter() param 2 expects Lambda, got ColSpec"
}
```

### Phase 2: Semantic (compile method)

Existing `compile*` methods run for deep validation (column existence, lambda body type-checking).

### Post-check: Return type assertion

```java
PureType declared = sig.returnType();
switch (declared) {
    case RelationIdentity() -> assert output.relationType().equals(input.relationType());
    case RelationSubset()   -> assert input.relationType().columns().keySet()
                                         .containsAll(output.relationType().columns().keySet());
    case Scalar(var t)      -> assert output.scalarType() != null;
    // ...
}
```

---

## What This Enables

| Capability | How |
|------------|-----|
| **LSP hover** | Show `pureSignature` on hover |
| **LSP completions** | Enumerate `registry.all()` with typed params |
| **Better errors** | `"groupBy() param 2 expects ColSpecArray, got Lambda"` |
| **NLQ grounding** | LLM sees formal signatures |
| **Self-documenting** | Registry IS the language spec |
| **Compiler self-checks** | Return type assertions catch bugs |

---

## Implementation Plan

### Phase 1: Core types + scalar functions
1. Create `PureType`, `Param`, `BuiltinFunction` records
2. Register all ~90 scalar/aggregate/window functions
3. Wire into TypeChecker: replace type-propagating case block
4. All existing tests pass unchanged

### Phase 2: Relation function signatures
5. Register all ~20 relation functions
6. Add structural pre-validation (Phase 1 checks)
7. Add return type post-assertions

### Phase 3: Consumers
8. LSP hover + completions
9. NLQ function catalog
10. PCT alignment tracking

---

## Open Questions

1. **Overloaded functions** — `project` has legacy (lambdas + aliases) and relation API (`ColSpec`) forms. Store multiple signatures per name, resolve by argument shape?

2. **`plus` polymorphism** — string concat or numeric addition depending on operand type. Keep as `Any` params with `Propagated` return, or model the overload?

3. **Passthrough** — today unknown functions fall through to SQL. Should the registry be the gatekeeper, or keep the open `default` case?
