# Legend Lite — Architectural Invariants

> **This file is read by AI coding assistants. Follow these rules strictly.**
>
> Active architectural plans:
> - `docs/pipeline-architecture.md` — backend (HIR → MIR → SQL).
> - `docs/frontend-architecture.md` — frontend (text → HIR).
>
> Read the relevant plan before any work touching that layer.

## Pipeline

Legend Lite has **two compilers** wrapped around a midend + backend.
Frontend = text → typed HIR. Midend = HIR → MIR. Backend = MIR → SQL.

```
text                                                       [FRONTEND]
  → Parser              [parser/]      text → AST                ┐
  → NameResolver        [parser/]      AST → AST (imports → FQN) │ Element
  → PureModelBuilder    [model/]       AST → CompiledElements    │ Compiler
  → MappingNormalizer   [compiler/]    mappings → NormalizedMapping ┘
  → TypeChecker         [compiler/]    ValueSpec + model → HIR   ┐ Expression
       └ checkers/      [compiler/checkers/]  per-native dispatch│ Compiler
  → MappingResolver     [compiler/]    HIR → resolved HIR        ┘
                                                           [MIDEND]
  → Lowerer             [plan/lowering/]  HIR → MIR
                                                           [BACKEND]
  → Dialect             [sqlgen/]         MIR → SQL
  → Executor            [executor/]       SQL → results
```

**Element Compiler** builds the typed model: classes, associations,
mappings, stores, services, runtimes, profiles, databases, function
signatures. Output: a populated `ModelContext`.

**Expression Compiler** type-checks queries and embedded expressions
against the model. Output: typed HIR.

The two are recursive: Element compilation triggers Expression
compilation for embedded expressions (mapping transforms, property
defaults, function bodies); Expression compilation may lazy-load classes
(see invariant 5).

`PlanGenerator` is a thin orchestrator wrapping the midend + backend
calls. It is not its own layer.

## Layer ownership (the contract)

| Layer | Owns | Forbidden |
|---|---|---|
| **Parser** | text → AST | type info, semantic decisions |
| **NameResolver** | imports → FQN over AST | consulting the model; type checking |
| **PureModelBuilder** (Element Compiler) | AST defs → `CompiledElement`s in `ModelContext` | type-checking expression bodies (deferred to Expression Compiler) |
| **MappingNormalizer** | mapping decls → immutable `NormalizedMapping` | mutating the model; running after TypeChecker |
| **TypeChecker** (Expression Compiler) | `ValueSpec` + model → typed HIR; resolves overloads to `NativeFunctionDef`; computes types/multiplicities | mutating `ModelContext`; emitting MIR |
| **MappingResolver** | typed HIR → resolved HIR (physical store refs) | re-running type checks; mutating `NormalizedMapping` |
| **Lowerer** | HIR → MIR via lowering rules + binding tables | naming SQL functions; SQL syntax; importing `SQLDialect`; any `String` field flowing into MIR that encodes a SQL operation |
| **Dialect** | MIR → SQL via `render(SqlExpr)`, `render(SqlAggregate, distinct)`, `render(SqlRelation)` | inferring types; rewriting HIR; consulting the model |
| **Executor** | SQL string → results | everything above |

## Invariants

### 1. The Frontend does ALL AST walking and typing

The Frontend has two cooperating compilers:
- **Element Compiler** (`PureModelBuilder` + `MappingNormalizer`):
  builds the typed model from `PackageableElement` defs.
- **Expression Compiler** (`TypeChecker` + `compiler/checkers/` +
  `MappingResolver`): type-checks `ValueSpec` against the model,
  produces typed HIR.

Together they are the **single source of truth for types**:
- Every expression, every function call MUST get a `TypeInfo`.
- Every overload MUST resolve to a `NativeFunctionDef`.
- Every variant access, cast target, and list type MUST resolve.
- If TypeInfo is missing, **the Frontend has a bug — fix the Frontend**
  (usually `TypeChecker` or the relevant `*Checker` subclass).

Legacy comments referring to "the Compiler" or "CleanCompiler" mean
`PureModelBuilder` (the Element Compiler entry point). The class named
`CleanCompiler` no longer exists.

### 2. Lowerer does no type inference

The Lowerer reads the **annotated HIR** — structure from typed nodes, types
and resolved metadata from TypeInfo and `NativeFunctionDef`.

The Lowerer MUST NOT:
- Infer or resolve types (no model lookups, no type compatibility checks).
- Validate correctness (that's the Compiler's job).
- Inspect HIR for **type** dispatch (no `instanceof CInteger` to decide MIR shape).
- Parse function names to guess types (no `funcName.contains("Int")`).
- Contain any SQL syntax or SQL function names.
- Import `SQLDialect`.

The Lowerer MAY:
- Read HIR structure: function names, parameters, lambda bodies, ColSpec
  names, nesting.
- Read TypeInfo annotations: expression types, store resolutions, sort specs.
- Pattern-match on typed HIR node kinds for **structural** dispatch.
- Use the binding tables (`NativeBindingTable`, `AggregateBindingTable`,
  `WindowBindingTable`) to map a resolved `NativeFunctionDef` to the
  appropriate typed MIR variant.

The Lowerer emits typed MIR records — never raw SQL, never SQL function
names, never `FunctionCall("name", args)` with a name string the dialect
will pattern-match.

### 3. Dialect owns ALL SQL rendering

The Dialect (`SQLDialect`) owns every SQL-specific decision:
- Type name mapping: `sqlTypeName("Integer")` → `BIGINT`.
- Keyword spellings, function names, syntactic quirks.
- Dialect-specific decompositions (WEEK diff, lpad safe, timeBucket) live
  **here**, not in lowering.

There are exactly three rendering entry points:
- `dialect.render(SqlExpr e)` — for scalar expressions.
- `dialect.render(SqlAggregate agg, boolean distinct)` — for aggregates.
- `dialect.render(SqlRelation r)` — for relational nodes.

Each is a pattern match over the corresponding sealed hierarchy. The MIR
root interfaces (`SqlExpr`, `SqlAggregate`, `WindowAggregate`,
`SqlRelation`) carry **explicit `permits` clauses** naming every
variant. Render methods are switch **expressions** (not statements) with
**no `default ->` arm**. javac then enforces exhaustiveness: adding a
variant without a render arm fails the build.

**SQLite-style "unsupported":** when a dialect genuinely cannot express
a variant (e.g. SQLite has no list lambdas), the arm **throws**
`UnsupportedOperationException`. That is still an arm — it satisfies
exhaustiveness. `default ->` is not an acceptable substitute.

### 3a. The MIR is closed and pure data

The MIR types (`SqlExpr`, `SqlAggregate`, `SqlRelation`, plus their record
variants) are **sealed** and **pure data**:

- **No method on a MIR type returns a SQL string.** No `toSql(...)`. No
  `String render()`. No `String emit()`.
- **No MIR type imports or references `SQLDialect`.** Records do not know
  dialects exist.
- **No MIR record has a `String` field encoding a SQL operation.** That
  means no `String name`, no `String funcName`, no `String op`, no `String
  sqlName`. The single carve-out is `SqlExpr.Cast(expr, pureTypeName)`,
  where `pureTypeName` is a Pure type name (`"Integer"`, `"String"`)
  mapped by `dialect.sqlTypeName()`. Pure type names are not SQL.
- **No `FunctionCall(String name, args)` catch-all.** Every operation has
  its own typed record. Adding a new Pure native = adding a new MIR
  variant + a new arm in `dialect.render`. The compiler enforces this.
- **Lambdas live in MIR as data.** `SqlExpr.LambdaExpr(params, body)` is
  the only admissible shape. The body is a `SqlExpr` produced by
  recursive scalar lowering after binding the lambda parameters. **MIR
  never holds a Pure AST node.** When a typed variant takes a lambda
  (e.g. `ListReduce`, `ListTransform`), the field is `SqlExpr` and the
  lowering must populate it with a `LambdaExpr`.
- **New variant = data-only record + new arm in dialect's pattern match.**
  The variant must contain no SQL syntax — not even "ANSI-universal" forms
  like `(a + b)`. SQL syntax lives only inside `dialect.render(...)`.
- **New dialect = one new class implementing the three `render` methods.**
  Nothing in MIR changes.

**Stop signs** — if you find yourself writing any of these, pause and
re-read this section:

- `record Foo(...) implements SqlExpr { String toSql(...) {...} }`
- `new SqlExpr.FunctionCall("someFunc", args)` in a lowering
- `private static String mapXxxName(String pureName)` in a lowering
- `default ->` in a dialect render method (add a real arm; throw if
  unsupported)
- `sealed interface ... { }` without a `permits` clause
- `dialect.renderFunction("name", args)` from anywhere outside the
  dialect itself (Phase D removes this method entirely)

The plan is `docs/pipeline-architecture.md`.

### 4. NO FALLBACKS. NO DEFAULTING.

- The **whole point** of the Compiler is to catch mistakes early.
- If a type is unknown, **fail** — do not guess, do not default, do not fall back.
- If TypeInfo is missing, **fix the Compiler** — do not work around it downstream.
- If a binding is missing for a resolved overload, **throw** — do not fall
  through to a stringly-typed catch-all.
- Every defaulting branch is a bug hiding behind a safety net.

### 5. Lazy loading of user packageable elements

Cross-project dependencies must not force-load the whole transitive graph. When
project A references a class in project B, compiling A must not require
loading every class in B. **`NameRef`, `Type.ClassType(fqn)`,
`Type.EnumType(fqn)`, and `PureClass.superClassFqns` exist specifically to
carry FQN references without forcing the target to load.**

**Split by origin:**

- **Platform types** — `LClass`, `Primitive`, platform enums, everything
  declared in `BuiltinClassRegistry`. Always loaded at JVM start (bootstrap).
  **Safe to eagerly classify and dispatch.** `NameResolver` promoting
  `NameRef("Relation")` → `LClass.RELATION` is correct because `LClass`
  is always in scope.
- **User types** — user-declared classes, enums, functions. Referenced by
  FQN. **Must not be eagerly dereferenced into resolved Java objects.** A
  `Type.ClassType("my::app::Person")` is just an FQN string in a typed
  wrapper; it does NOT imply `my::app::Person` is loaded.

**The lazy contract:**

- Structural access (`findProperty`, `allProperties`, `isClassSubtype`,
  superclass-chain walks) MUST go through `ModelContext.findClass(fqn)` /
  `findEnum(fqn)` / `findFunction(fqn)`. These methods are the sole layer
  that owns load triggering.
- Long-lived fields hold **FQN strings** or FQN-wrapping types (`ClassType`,
  `EnumType`, `NameRef`), never resolved `PureClass` / `PureEnum` /
  `FunctionDefinition` references. `PureClass.superClassFqns: List<String>`
  is the canonical example.
- Classification (`NameRef` → `ClassType` / `LClass` / `Primitive`) is a
  controlled boundary at property-build time
  (`PureModelBuilder.substituteTypeVarsAndClassify`) and at expression
  type-checking. Outside those boundaries, keep `NameRef` unresolved.

**Never:**

- Store `PureClass` / `PureEnum` / `FunctionDefinition` as fields of another
  `PureClass`, `PropertyBuilder`, or long-lived container. Store the FQN
  and resolve lazily through `ModelContext`.
- Walk a dependency graph manually (e.g., collect all classes via field
  access). Go through `ModelContext.findClass(fqn)` every step.
- Mix platform and user handling in an eager-resolution path that always
  loads the target. Guard platform-only promotion with
  `LClass.findByFqn(fqn).isPresent()` or equivalent so user FQNs fall
  through unchanged.
- Force user-type load just to dispatch on shape (e.g., "is this a list?").
  Dispatch on the `Type` identity (`LClass`, `Primitive`,
  `ClassType.qualifiedName()`), not on the resolved object.

**When you genuinely need a user class's structure**, call
`modelContext.findClass(fqn)` at the exact use site and let the context
decide whether to lazy-load. Don't cache the result across unrelated
operations.

**Automated guards** — two tests enforce this rule at build time:

- **`NoEagerTypeReferencesTest`** — structural: reflects over every compiled
  class and fails if any field outside a tiny field-level allowlist has a
  resolved `PureClass` / `PureEnum` reference (directly or nested in `List`
  / `Map` / `Optional` / arrays). Adding your class to the allowlist is
  **not** the fix; converting the field to an FQN `String` is.
- **`NoEagerUserClassLoadsTest`** — behavioral: wraps `ModelContext` in a
  call-recording decorator, compiles a query that touches one warm class,
  and fails if `findClass` was invoked on any of several cold classes.
  Catches eager traversals that the structural guard can't see.

## MIR rules (canonical reference)

- `Cast(expr, pureTypeName)` — Pure type name, Dialect maps via
  `sqlTypeName()`. **Only** string-typed dispatch surface in the MIR.
- Every other operation is a typed sum-type variant. No string names.
- `VariantTextExtract` — not `JsonAccess` (dialect-agnostic naming).
- `ToVariant` — for `toVariant()` Pure function; Dialect renders to
  `CAST AS JSON` / `VARIANT` / equivalent.
- NO `RawCast` — removed because it bypassed dialect mapping.

## Common mistakes (don't repeat)

1. **`(int) longValue`** — Java boxed `Long` can't raw-cast to `int`. Use `.intValue()`.
2. **Hardcoding SQL in lowering** — emit a typed MIR variant; the Dialect renders.
3. **Naming SQL functions in lowering** — no `FunctionCall("name", args)`.
   Use a typed variant or a binding-table lookup.
4. **`String mapXxxName(String pureName)` helpers in lowering** — these are
   the smoking gun of stringly-typed dispatch leaking SQL knowledge into
   the wrong layer. Delete them; replace with typed MIR variants.
5. **Adding normalizations instead of fixing root cause** — fix the actual
   generation.
6. **Adding fallbacks/defaults** — fail loudly; fix the Compiler instead.
7. **Fixing fallbacks by changing the default** — if you find a
   fallback/default branch being hit, do NOT change what it defaults to.
   Instead: (a) make it throw, (b) find why the Compiler produced
   null/missing TypeInfo or no binding, (c) fix the upstream layer. The
   fallback existing at all is the bug.
8. **`default ->` in a dialect render method** — exhaustiveness is the
   type-safety guarantee. Add the missing arm.
9. **Type inference in the Lowerer** — Lowerer reads typed HIR for
   structure, never infers types. If the Lowerer needs a type, it comes
   from TypeInfo or `NativeFunctionDef`.
10. **Making the Compiler lenient on missing model elements** — the Compiler
    MUST throw if a referenced class, property, or type is not found in the
    model context. This is literally the compiler's job. If a test fails
    because a class isn't found, **fix the test to set up the model
    correctly** (add the class to `commonClassDefs()` or `TypeEnvironment`).
    NEVER make the Compiler silently degrade (e.g., fall back to `ANY`,
    skip compilation). Silent degradation hides bugs and causes downstream
    issues.
