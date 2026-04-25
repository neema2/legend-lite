---
description: Frontend architecture — two compilers, six phases, typed HIR as the output contract
status: active
companion: pipeline-architecture.md
---

# Frontend Architecture

> Companion to `pipeline-architecture.md`. That doc owns HIR → MIR → SQL
> (midend + backend). This one owns text → HIR (frontend).

## 1. The two-compiler structure

Legend Lite has **two distinct compilers**, not one. Industry convention
calls these the **declaration compiler** (Roslyn) / **namer** (Scala) for
the outer one, and the **expression compiler** / **typer** for the inner.

### Element Compiler (outer)

- **Input**: `List<PackageableElement>` AST (classes, associations,
  mappings, stores, services, runtimes, connections, profiles, databases,
  functions).
- **Output**: a populated `ModelContext` whose entries are
  `CompiledElement`s (`CompiledClass`, `CompiledMapping`, `CompiledDatabase`, ...).
- **Job**: build the typed model — resolve imports, populate the symbol
  table, type-check class hierarchies and property types, normalize mapping
  declarations.
- **Calls into**: the Expression Compiler when a packageable element
  contains an embedded expression (mapping transform, property default,
  derived property body, function body, service query).

### Expression Compiler (inner)

- **Input**: `ValueSpecification` AST + `ModelContext` (from outer) +
  `TypeCheckEnv` (variable bindings).
- **Output**: typed HIR (`TypedSpec` / `CompiledExpression`).
- **Job**: type-check, infer types, dispatch per Pure native to a checker,
  produce the typed expression tree.
- **Idempotent over `ModelContext`** — never mutates the model; only reads.

The pair is recursive: Element compilation may invoke Expression
compilation (e.g. compiling a mapping transform); Expression compilation
may trigger Element loading (e.g. lazy class lookup via
`ModelContext.findClass(fqn)`). The recursion is bounded by the lazy
loading rules in AGENTS.md §5.

## 2. The full pipeline

```
text
  ┌─── ELEMENT COMPILER ─────────────────────────────────────────────┐
  │  → Parser              [parser/]      text → AST element defs    │
  │  → NameResolver        [parser/*]     defs → defs (imports → FQN)│
  │  → PureModelBuilder    [model/]       defs → CompiledElements    │
  │  → MappingNormalizer   [compiler/]    mappings → NormalizedMapping│
  └──────────────────────────────────────────────────────────────────┘
  ┌─── EXPRESSION COMPILER ──────────────────────────────────────────┐
  │  → Parser              [parser/]      query text → ValueSpec AST │
  │  → NameResolver        [parser/*]     ValueSpec → ValueSpec (FQN)│
  │  → TypeChecker         [compiler/]    ValueSpec + model → typed HIR
  │       └ checkers/      [compiler/checkers/]  per-native dispatch │
  │  → MappingResolver     [compiler/]    typed HIR + mapping → resolved HIR
  └──────────────────────────────────────────────────────────────────┘
  ┌─── MIDEND + BACKEND (see pipeline-architecture.md) ──────────────┐
  │  → Lowerer             [plan/lowering/]   HIR → MIR              │
  │  → Dialect             [sqlgen/]          MIR → SQL              │
  │  → Executor            [executor/]        SQL → results          │
  └──────────────────────────────────────────────────────────────────┘
```

`*` = `NameResolver` is in `parser/` for historical reasons but is not
part of parsing. Future cleanup may move it.

## 3. Phase responsibilities

### 3.1 Parser
Pure ANTLR-style parsing. No semantic decisions. Multiple entry points
(`PureModelParser` for element defs, `PureQueryParser` for queries,
`PureNativeSignatureParser` for native function signatures).

### 3.2 NameResolver
Imports → FQN. Two entry points:
- `resolveDefinitions(defs, imports, knownFqns)` — element compilation.
- `resolveQuery(valSpec, imports, knownFqns)` — expression compilation.

Resolution rule (mirrors legend-engine `CompileContext.resolve`):
- Already-FQN (contains `::`) → as-is
- Simple name + each wildcard import → check against `knownFqns`
- 0 matches → as-is (may be a primitive)
- 1 match → return the FQN
- &gt;1 matches → ambiguity error

NameResolver does **not** consult the model; it works on the raw AST and
the precomputed FQN set. This makes it order-independent with respect
to model loading.

### 3.3 PureModelBuilder (Element Compiler core)
Build `CompiledElement`s from AST defs. Populates the symbol table.
Owns the lazy-loading boundary (see AGENTS.md §5). Each
`CompiledClass`/`CompiledMapping`/etc. is a typed wrapper around the
def + resolved metadata.

This is what AGENTS.md and other docs sometimes call "the Compiler" or
"CleanCompiler" — the actual class is `PureModelBuilder`. References to
`CleanCompiler` in legacy comments are stale; they mean
`PureModelBuilder`.

### 3.4 MappingNormalizer
Desugars mapping declarations into a canonical, immutable
`NormalizedMapping` snapshot. Conventions:
- M2M chain resolution (fills `targetClass` + `sourceMapping`).
- Relational filter conversion (`~filter` → `ValueSpecification`).
- MappingExpression construction for downstream `TypeChecker` use.
- Association join resolution (pre-resolves traversals).

After this phase, `MappingRegistry` is **read-only**. Industry parallel:
this is "AST desugaring" / "syntactic lowering." (Note: Rust calls
*this* phase "lowering" too, distinct from our LLVM-style backend
"lowering.")

### 3.5 TypeChecker (Expression Compiler core)
Walks `ValueSpecification` AST + `ModelContext` and produces typed HIR.
Dispatch is per Pure native via `checkers/` (one class per native or
native family: `GetAllChecker`, `ProjectChecker`, `FilterChecker`,
`ScalarChecker`, `FoldChecker`, `JoinChecker`, ...).

Each checker:
- Validates argument types against the `NativeFunctionDef` signature.
- Unifies type variables and multiplicity variables.
- Resolves output type via `resolveOutput`.
- Returns a typed HIR node (`TypedSpec` subtype).

This is the type-system surface — no other phase infers or resolves
types. AGENTS.md §1 invariant: TypeChecker is the single source of truth.

### 3.6 MappingResolver
Runs after TypeChecker. Walks the typed HIR and resolves Pure
property/class references to physical store concepts (table, column,
join). Produces `ResolvedExpression` annotations alongside the typed
nodes.

Industry parallel: post-type semantic analysis / late binding pass.
Roslyn calls these "binding pass 2."

## 4. Frontend invariants

These mirror the backend invariants in `pipeline-architecture.md` and
should evolve toward the same level of rigor.

### 4.1 NameResolver is model-free
NameResolver MUST NOT consult `ModelContext`. It operates on the raw
AST + a precomputed FQN set + the import list. This guarantees
NameResolver runs before model construction without circular
dependencies.

### 4.2 Element Compiler MUST NOT depend on Expression Compiler at
declaration level
`PureModelBuilder` produces `CompiledElement`s without compiling any
expression bodies. Expression compilation is **deferred** until
something demands a typed body (a query runs, a service is invoked, an
association is traversed).

This is the lazy-loading rule extended: just as classes load lazily,
expression bodies type-check lazily.

### 4.3 TypeChecker MUST NOT mutate `ModelContext`
TypeChecker reads the model, never writes. All "what's the type of x"
questions go through cached `CompiledElement`s. New entries appear only
via the Element Compiler.

### 4.4 MappingResolver is read-only over `NormalizedMapping`
The `NormalizedMapping` produced in §3.4 is immutable. MappingResolver
queries it; it does not synthesize new mapping entries. New mapping
shapes require extending `MappingNormalizer`.

### 4.5 NO FALLBACKS at any phase
Same rule as backend AGENTS.md §4. Each phase fails loud on missing
inputs. No `default ->` in checker dispatch. No silent type degradation
to `ANY`.

### 4.6 Stringly-typed dispatch is a smell here too
`AbstractChecker` itself flags this in its javadoc:
> Dispatch on `Type.Parameterized#rawType()` is centralized in
> `structuralMatch`, `unifyType`, `resolve`: these are the only sites
> that know about signature-layer pseudo-types (`Relation`, `ColSpec`,
> `_Window`, etc.) by name. A future refactor may replace the string
> dispatch with a typed `rawType` enum.

Same disease as the backend `FunctionCall(String, args)` we are
eliminating. Future frontend cleanup target: typed `RawType` enum +
sealed `Type.Parameterized` permits.

## 5. Known structural issues (not blocking)

These are accurate observations, not action items. Each is small and
cosmetic.

1. **`NameResolver` lives in `parser/`** — it's not parsing. Belongs
   in `compiler/resolve/` or its own `resolve/` package.
2. **`PureModelBuilder` lives in `model/`** — it's the Element
   Compiler. AGENTS.md should stop calling it "CleanCompiler."
3. **`compiler/` package conflates desugar (`MappingNormalizer`),
   typecheck (`TypeChecker`), and post-type (`MappingResolver`).**
   Three subpackages would clarify ownership.
4. **`compiler/checkers/` is fine** — it's the per-native dispatch
   table for the Expression Compiler. Industry pattern (Scala's
   `Typer`, Roslyn's binders).
5. **`AbstractChecker` stringly-typed `rawType()` dispatch** —
   typed-enum migration is a future cleanup, parallel to backend
   MIR closure.
6. **Pipeline is described as one box in AGENTS.md** — fixed by the
   diagram update accompanying this document.

## 6. Vocabulary

To use industry terms consistently:

| Term | Meaning here |
|---|---|
| **Frontend** | Phases 3.1–3.6: parser through MappingResolver. Source language → fully resolved typed HIR. |
| **Midend** | Phases that operate on typed IR without producing target code. `Lowerer` (HIR → MIR) is midend; would also include any future IR optimization passes. |
| **Backend** | `Dialect` (MIR → SQL). Target codegen. |
| **HIR** | The typed output of the Frontend. Holds Pure semantics: lambdas, multiplicities, navigation paths, classes. |
| **MIR** | The typed output of the Lowerer. Holds SQL relational algebra, dialect-free. See `pipeline-architecture.md`. |
| **Element Compiler** | Outer compiler: `PackageableElement` AST → `CompiledElement`s in `ModelContext`. Phases 3.1–3.4 + lazy invocations of inner. |
| **Expression Compiler** | Inner compiler: `ValueSpecification` AST + `ModelContext` → typed HIR. Phases 3.5–3.6 + reuses 3.1–3.2 for query parsing. |

## 7. Out of scope

- Code package moves (NameResolver to `compiler/resolve/`, etc.) —
  cosmetic, not now.
- Typed `RawType` enum — flagged as future cleanup, not blocking.
- Frontend refactor sequencing — to be planned after the backend MIR
  closure (`pipeline-architecture.md` Phases A–F) lands.
