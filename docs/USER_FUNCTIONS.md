# User-Defined Functions

Complete reference for user-defined Pure functions in Legend Lite.

## Syntax

```pure
function <package>::<name>(<params>):<returnType>[<multiplicity>]
{
    <body>
}
```

**Example:**
```pure
function risk::boundedScore(value: Integer[1], cap: Integer[1]):Integer[1]
{
    if($value > $cap, |$cap, |$value)
}
```

---

## Parameters

### Scalar Parameters

Standard typed parameters with multiplicity.

```pure
function test::fullName(first: String[1], last: String[1]):String[1]
{
    $first + ' ' + $last
}

// Call: test::fullName($x.firstName, $x.lastName)
```

### Function-Typed Parameters (Higher-Order)

Lambda parameters declared with `{ParamTypes -> ReturnType}` syntax.

```pure
function test::apply(f: {Integer[1]->Integer[1]}[1], x: Integer[1]):Integer[1]
{
    $f->eval($x)
}

// Call: test::apply({y|$y * 2}, $x.age)
```

**Validation:**
- **Arity** is checked at compile time — passing `{a,b|...}` where `{Integer[1]->Integer[1]}` is expected fails
- **Parameter types are inferred** (bidirectional) — lambda params like `y` in `{y|$y * 2}` are untyped in Pure syntax; the declared `FunctionType` pushes `Integer[1]` into `y` so the body compiles with the correct type context
- **Return type** is checked — if the body returns `Integer` but the `FunctionType` declares `->Boolean[1]`, a compile error is thrown

### Class-Typed Parameters

Parameters can accept class instances, including full query chains.

```pure
function test::filterPeople(people: Person[*]):Person[*]
{
    $people->filter(p|$p.age >= 18)
}

// Call: test::filterPeople(model::Person.all())->project([p|$p.firstName], ['name'])
```

When the body transforms a class into a Relation (via `project`), use `Relation<(schema)>` as the return type:

```pure
function test::getNames(people: Person[*]):Relation<(name:String)>[1]
{
    $people->project([p|$p.firstName], ['name'])
}
```

### Relation-Typed Parameters with Schema

Typed `Relation<(col:Type)>` parameters enable **compile-time schema validation** at the function boundary.

```pure
function test::filterByName(data: Relation<(FIRST_NAME:String, AGE_VAL:Integer)>[1]):Relation<(FIRST_NAME:String)>[1]
{
    $data->filter(x|$x.FIRST_NAME == 'John')
}

// Call: test::filterByName(#>{store::DB.T_PERSON}#->select(~[FIRST_NAME, AGE_VAL]))
```

**Bare parameters** (`Any[*]`) skip boundary validation — schema errors are caught later during structural inlining:

```pure
function test::untyped(data: Any[*]):Any[*]
{
    $data->filter(x|$x.AGE_VAL > 30)
}
```

---

## Type Checking

### Type Hierarchy (Subtype Rules)

All type compatibility checks use a unified `isSubtype` method backed by the canonical `Primitive.isSubtypeOf` hierarchy:

```
Any
├── Number
│   ├── Integer
│   │   ├── INT64
│   │   └── INT128
│   ├── Float
│   └── Decimal
├── Date
│   ├── StrictDate
│   └── DateTime
├── String
├── Boolean
├── StrictTime
└── JSON
```

**Rules:**
- `Integer` argument satisfies `Number` parameter
- `StrictDate` argument satisfies `Date` parameter
- `Employee` argument satisfies `Person` parameter (class hierarchy via superclass walk)
- `String` argument does **not** satisfy `Integer` parameter → compile error

### Parameter Type Checking (Step 4)

When a user function is called, each argument's actual type is checked against the declared parameter type:

| Declared | Actual | Result |
|----------|--------|--------|
| `Integer[1]` | Integer | ✅ Exact match |
| `Number[1]` | Integer | ✅ Subtype |
| `Person[*]` | Employee | ✅ Class hierarchy |
| `String[1]` | Integer | ❌ Type mismatch |
| `Person[*]` | Firm | ❌ Unrelated class |
| `Any[*]` | anything | ✅ Always |

### Relation Schema Checking

For `Relation<(col:Type)>` parameters, **superset + column type hierarchy** semantics apply:

| Declared Schema | Actual Schema | Result |
|----------------|---------------|--------|
| `(FIRST_NAME:String)` | `(FIRST_NAME:String, AGE_VAL:Integer)` | ✅ Superset OK |
| `(FIRST_NAME:String)` | `(AGE_VAL:Integer)` | ❌ Missing column `FIRST_NAME` |
| `(AGE_VAL:Number)` | `(AGE_VAL:Integer)` | ✅ Integer is subtype of Number |
| `(AGE_VAL:String)` | `(AGE_VAL:Integer)` | ❌ Type mismatch |

Error messages are precise:
```
Function 'test::f' parameter 'data': schema mismatch — missing column 'FIRST_NAME'. Available columns: [AGE_VAL]
Function 'test::f' parameter 'data': schema mismatch — column 'AGE_VAL' expects String but got Integer
```

### Return Type Checking (Step 9)

The body's actual return type is validated against the declared return type.

**For scalar return types:** Standard subtype check — `Integer` body satisfies `Number` return, but not `Boolean`.

**For `Relation<(schema)>` return types:** Covariant check — the body's actual schema must be a **superset** of the declared return schema. This means the function guarantees "at minimum these columns" while the body may produce more.

```pure
// Declares return (FIRST_NAME:String), body actually returns (FIRST_NAME, AGE_VAL)
// Covariant: body ⊇ declared → OK
function test::filterAge(data: Relation<(FIRST_NAME:String, AGE_VAL:Integer)>[1]):Relation<(FIRST_NAME:String)>[1]
{
    $data->filter(x|$x.AGE_VAL > 30)
}
```

---

## Multiplicity Checking

Parameter multiplicity is validated: the actual argument's multiplicity range must fit **within** the declared parameter's range.

| Declared | `[1]` actual | `[0..1]` actual | `[*]` actual |
|----------|:---:|:---:|:---:|
| **`[1]`** | ✅ | ❌ lower too low | ❌ lower too low |
| **`[0..1]`** | ✅ | ✅ | ❌ upper too high |
| **`[*]`** | ✅ | ✅ | ✅ |
| **`[1..*]`** | ✅ | ❌ lower too low | ❌ lower too low |

**Semantics:**
- `[1]` = exactly one, guaranteed non-null
- `[0..1]` = optional, might be absent
- `[*]` = zero or more
- `[1..*]` = at least one, possibly many

Error messages:
```
Function 'test::f' parameter 'x' expects multiplicity Integer[1] but got [*]
```

---

## Function Bodies

### Single Expression

```pure
function test::double(x: Integer[1]):Integer[1]
{
    $x * 2
}
```

### Let Chains (Multi-Statement)

Intermediate `let` bindings create local variables. The last expression (or last `let` value) is the return value.

```pure
function test::compute(x: Integer[1]):Integer[1]
{
    let doubled = $x * 2;
    let result = $doubled + 10;
    $result;
}
```

### Query-Returning Bodies

Functions can return class queries or relation expressions. Callers can chain additional operations.

```pure
function test::adults():Person[*]
{
    model::Person.all()->filter(p|$p.age >= 18)
}

// Caller chains project:
|test::adults()->project([p|$p.firstName], ['name'])

// Caller chains additional filter + sort:
|test::adults()->filter(x|$x.age > 30)->project([p|$p.firstName], ['name'])->sort('name', SortDirection.ASC)
```

**Relation-returning:**
```pure
function test::personRelation():Relation<(FIRST_NAME:String, AGE_VAL:Integer)>[1]
{
    #>{store::PersonDatabase.T_PERSON}#->select(~[FIRST_NAME, AGE_VAL])
}

// Caller chains filter:
|test::personRelation()->filter(x|$x.AGE_VAL > 30)
```

---

## Overloading

### By Arity

```pure
function test::fmt(val: String[1]):String[1]
{
    $val + '!'
}

function test::fmt(val: String[1], suffix: String[1]):String[1]
{
    $val + $suffix
}

// test::fmt('hello')       → 'hello!'
// test::fmt('hello', '..') → 'hello..'
```

### By Type

When multiple overloads have the same arity, the compiler scores each candidate by how many argument types match and picks the best fit.

```pure
function test::show(x: Integer[1]):Integer[1]  { $x * 10 }
function test::show(x: String[1]):String[1]    { $x + '!' }

// test::show($p.age)       → Integer overload
// test::show($p.firstName) → String overload
```

**Ambiguous overloads** (identical signatures) are rejected at compile time:
```
Ambiguous overload for 'test::dup': multiple candidates match with 2 matching params
```

---

## Composition Patterns

### Nested Calls (f calls g)

```pure
function test::inner(x: Integer[1]):Integer[1]  { $x * 3 }
function test::outer(x: Integer[1]):Integer[1]  { test::inner($x) + 1 }
```

### Calling Built-in Functions

User functions can call any built-in function:

```pure
function test::isAdult(age: Integer[1]):Boolean[1]
{
    $age >= 18
}
```

### Higher-Order Composition

```pure
function test::apply(f: {Integer[1]->Integer[1]}[1], x: Integer[1]):Integer[1]
{
    $f->eval($x)
}

function test::doubleApply(x: Integer[1]):Integer[1]
{
    test::apply({y|$y * 2}, $x)
}
```

### Mixed: Scalar + Query Functions

```pure
function test::adults():Person[*]
{
    model::Person.all()->filter(p|$p.age >= 28)
}

function test::doubleAge(n: Integer[1]):Integer[1]
{
    $n * 2
}

// Use both: query fn for source, scalar fn in projection
|test::adults()->project([p|test::doubleAge($p.age)], ['doubled'])
```

### Realistic: Higher-Order with Clamping

```pure
function risk::boundedScore(scoreFn: {Integer[1]->Integer[1]}[1], value: Integer[1], cap: Integer[1]):Integer[1]
{
    let raw = $scoreFn->eval($value);
    if($raw < 0, |0, |if($raw > $cap, |$cap, |$raw));
}

// Call: risk::boundedScore({x|$x * 3}, $p.age, 100)
```

---

## Error Cases

| Error | Trigger | Message |
|-------|---------|---------|
| **Arity mismatch** | Wrong number of arguments | `No overload of 'test::f' accepts 2 argument(s)` |
| **Type mismatch** | Incompatible argument type | `parameter 'x' expects String but got Integer` |
| **Multiplicity mismatch** | Wrong cardinality | `parameter 'x' expects multiplicity Integer[1] but got [*]` |
| **Return type mismatch** | Body returns wrong type | `declares return type Boolean but body returns Integer` |
| **Schema: missing column** | Relation missing required column | `schema mismatch — missing column 'FIRST_NAME'` |
| **Schema: type mismatch** | Column type incompatible | `schema mismatch — column 'AGE_VAL' expects String but got Integer` |
| **Lambda arity** | Wrong lambda param count | `expects a lambda with 1 param(s), but got 2` |
| **Lambda return type** | Lambda body returns wrong type | `Lambda return type mismatch: expected Boolean but body returns Integer` |
| **Ambiguous overload** | Multiple candidates tie | `Ambiguous overload for 'test::f'` |
| **Recursion** | Infinite self-call | `inline depth` exceeded |
| **Empty body** | No expression in function | Parse error |

---

## Compiler Implementation

User functions are compiled via **inlining** in `TypeChecker.inlineUserFunction`. The 10-step pipeline:

| Step | Description |
|------|-------------|
| **1** | Look up function candidates by qualified name |
| **2** | Filter by arity |
| **3** | Compile arguments with bidirectional type info for lambda params |
| **4** | Type check: validate actual arg types vs declared param types (including Relation schema) |
| **4b** | Multiplicity check: actual range must fit within declared range |
| **5** | Get pre-resolved body (or parse from source) |
| **6** | Build bindings: `paramName → argument AST` |
| **7** | Substitute params in body statements (AST-level, capture-avoiding) |
| **8** | Compile substituted body with let-chaining |
| **9** | Return type validation (including covariant Relation schema check) |
| **10** | Stamp `TypeInfo` with `inlinedBody` for PlanGenerator |

**Key architectural property:** User functions are fully inlined — the caller receives the body's actual type and AST. PlanGenerator sees only the expanded expression, never the function call. This means SQL generation works transparently with all existing relational, filter, project, sort, and aggregation codepaths.

### Substitution Safety

Parameter substitution operates on the AST, not text. `$x` in a function body is replaced with the caller's argument AST node. This is:

- **Prefix-safe:** param `x` does not accidentally match variable `xx`
- **Capture-avoiding:** lambda params in the body shadow function params correctly

### Recursion Guard

A depth counter prevents infinite inlining. The limit is 32 nested inline levels, after which a compile error is thrown.
