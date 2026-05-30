---
description: End-to-end per-pass invariants. Ideal state only. Gap analyses live as separate audit docs.
status: active
related:
  - AGENTS.md — short rules-of-engagement
  - docs/pipeline-architecture.md — midend/backend layer contract (closed MIR)
  - docs/frontend-architecture.md — frontend narrative (text → HIR)
  - docs/audits/ — per-pass gap analyses (created as we audit each pass)
---

# Pipeline Invariants

> **This document declares what each pass MUST guarantee about its output.**
> It is forward-declared and aspirational: the current code may not yet
> satisfy every invariant. Per-pass gap analysis lives in `docs/audits/`,
> not here. When this doc and the code disagree, the code is the bug.
>
> **Lessons borrowed from sister projects appear inline**, e.g.
> `[← rustc HIR/MIR]`, `[← Catalyst Analyzer]`. Each citation marks an
> idea we've explicitly stolen. We are not inventing a new compiler design;
> we are translating known-good designs to our domain.

## 0. Why this document exists

We have repeatedly found ourselves in a state where a pass downstream
(e.g. `MappingResolverV2`) is **compensating** for a job left undone by
a pass upstream (e.g. `TypeChecker` emitting bare-string `associationPath`
instead of resolved hop references). Each compensation adds a side
channel; side channels accumulate; the system becomes opaque.

The cure is to pin down **per-pass invariants strong enough that
compensation is impossible**. If a pass declares "after I run, no
node carries an unresolved property reference", then no downstream pass
needs to handle that case — the type system rules it out.

This document is the place that pinning happens.

---

## 1. Foundational invariants (cross-cutting)

These hold across the entire pipeline, not just one pass.

### 1.1 Per-element discipline (lazy ↔ eager equivalence) `[← rustc query system]`

Every invariant in this document is stated **per-element**, not
"globally over the project". A class, function, association, or mapping
either has been advanced to a given phase or has not. There is no
global "the project has been type-checked" state.

This is the foundation that makes interactive (Interpreted++) and eager
(Bazel) modes behave identically:

- **Eager (Bazel)**: every element is advanced to its terminal phase
  before any query runs. The lazy code path is dormant.
- **Interactive**: elements are advanced to phases on demand, driven by
  what the user's current query needs. Phase advancement is memoized
  per element.

Both modes call the same `ModelContext.resolve(ref)` API and obtain
the same result. The only difference is whether `resolve` finds the
element pre-advanced or has to advance it now.

**Borrowed from rustc's query system**: the query `tcx.type_of(DefId)`
returns the same value whether the type was computed an hour ago or
this microsecond. The cache is observation-equivalent to recomputation.

### 1.2 Resolved state lives in TYPES, not flags `[← rustc HIR/MIR transition]`

When a pass's job is to **resolve** something (e.g. resolve property
paths to typed references), its output AST node type **does not have
the unresolved field**. Resolution is encoded by *which type the node
is*, not by a `boolean resolved` or `Optional<Resolved>` field.

Concretely: today we have

```java
record TypedPropertyAccess(
    TypedSpec source,
    String property,
    Optional<List<String>> associationPath,    // unresolved
    Optional<String> physicalColumn,           // resolved
    ExpressionType info) implements TypedSpec {}
```

This is the wrong shape. A node with `physicalColumn` populated and
`associationPath` empty is "resolved"; the opposite is "unresolved".
A reader has to inspect both fields to know what state it's in.

The right shape is **two distinct types**:

```java
sealed interface TypedPropertyAccess permits Unresolved, Resolved {}
record Unresolved(TypedSpec source, List<HopRef> path) implements TypedPropertyAccess {}
record Resolved(TypedSpec source, ColumnRef column) implements TypedPropertyAccess {}
```

After `MappingResolverV2`, every `TypedPropertyAccess` in the tree is
`Resolved` — by Java's type system, not by a flag. Downstream code
matches on `Resolved` and never sees `Unresolved`. A bug that leaves
something unresolved is a *javac error*, not a runtime null.

**Borrowed from rustc**: HIR has unresolved path expressions; after the
HIR-lowering pass, MIR has only resolved place expressions. The two
IRs are different types; conversion is a *transformation*, not a
mutation.

### 1.3 No string-typed model references after NameResolver `[← rustc DefId, Trino SymbolReference]`

Once `NameResolver` runs, every reference to a model element (class,
function, association, mapping, store, table) is an FQN — and from
the `SymbolTable` boundary onward, that FQN is interned to an integer
id. **No string lookup of model elements happens downstream of the
type-check boundary.**

The single carve-out: error messages may stringify FQNs for human
output. This is a presentation concern, not a dispatch concern.

This is the HIR analog of the existing MIR rule "no String fields
encode dispatch" (see `pipeline-architecture.md` §3). Both IRs are
closed and exhaustively typed; both forbid string-typed dispatch.

### 1.4 `TypedReference<T>` for cross-element references `[← rustc DefId, Catalyst AttributeReference]`

The HIR does **not** embed `PureClass`, `PureFunction`, `Association`,
or `Mapping` instances. Instead, every cross-element reference is a
`TypedReference<T>`:

```java
sealed interface TypedReference<T> permits Resolved, Unresolved {
    record Resolved<T>(int symbolId) implements TypedReference<T> {}
    record Unresolved<T>(String fqn) implements TypedReference<T> {}
}
```

(Sketch — the real design may use a different record shape; the
*semantic* is what matters: a reference is a *handle*, not the thing
itself.)

Consumers obtain the referenced element via `ModelContext.resolve(ref)`.
The `ModelContext` impl decides whether `resolve` is a memory map
lookup (eager mode), a content-addressed cache hit, or a project
flesh-out (lazy mode). Consumers neither know nor care.

Properties of this shape:
- **Tree comparison is cheap**: two HIR trees are equal iff their
  references' symbol ids match. No deep walk into shared model objects.
- **Serialization is trivial**: a HIR tree contains no model state, only
  references. Round-trip through a wire format is bit-stable.
- **Cross-project is free**: a reference's symbol id implicitly
  identifies its project (via the `SymbolTable`'s id allocation
  scheme). Resolving a reference whose project hasn't been loaded
  triggers project load.

### 1.5 Closed typed IRs at every layer

Every IR (`Compiled*`, `TypedSpec`/HIR, MIR `SqlExpr`/`SqlRelation`)
is a `sealed` hierarchy with `permits` clauses. Pattern-match
exhaustiveness is enforced by javac. Adding a new node kind without
updating consumers is a build failure, not a runtime surprise.

`pipeline-architecture.md` §3 declares this for the MIR. This document
extends the same rule to the HIR (`TypedSpec` hierarchy) and the
model (`Compiled*` hierarchy).

### 1.6 Read-only model contexts; mutation lives in builders

`ModelContext` is read-only. All mutation happens through builder
types (`PureModelBuilder`, etc.) that produce immutable model
snapshots. Passes consume a snapshot; they cannot mutate it.

Concretely: no field on `ModelContext` is non-final. No method on
`ModelContext` returns a mutable collection. No pass holds a
`PureModelBuilder` reference.

### 1.7 Everything is a function or expression `[← Lisp/Scheme]`

Classes and enums are **structural types only** — they describe shape,
they are not computation. Every other source-level concept in the
system is a function or expression:

- A query is a function body.
- A mapping is a function from store rows to class instances.
- A derived property is a function on its enclosing class.
- A relation API is a function returning `Relation<...>`.
- A store view is a function returning `Relation<...>`.
- A constraint is a function returning `Boolean`.

**There are multiple ways to produce class-typed values**, and a
mapping function is only one of them:

- **Mapping function**: a function from store rows to class instances
  (`Relation<C>`).
- **Constructor expression**: `^Class(prop1=expr1, prop2=expr2, ...)`
  produces a `C[1]` with the given args bound. No store, no relation —
  an in-memory instance.
- **Pipeline expression**: `source->extend(~p: e)->filter(...)` builds
  up a class-typed relation by composition.
- **User function returning class-typed**: any function whose return
  type is `C[..]` or `Relation<C>` is a producer; its bindings are
  inherited from its body.

There is **one code path** for compiling all of these. The pipeline
never branches on "is this a mapping vs a query vs a derived
property" — the type system distinguishes them via return type
(`Relation<C>` vs `C` vs `Boolean`), but the *compilation pipeline*
treats them identically. All producers of class-typed values are
subject to the same property-bindings rules (§1.8, §1.10).

Borrowed from Lisp/Scheme: program is data + functions, no
privileged forms. The same discipline applied to Legend: every
source-level construct is either a structural type (class, enum)
or a function/expression. The mapping DSL is sugar; the desugared
form is what the rest of the pipeline operates on (§1.9).

### 1.8 Property bindings `[← Calcite RelMetadataQuery, Catalyst AttributeMap]`

#### Mental model

Every class-instance producer is conceptually a
**(class + store + mapping + `.all()`-query) composite**. Producers
differ only in how that composite is **fragmented across source**:

- **Mapping function**: class declared separately, store declared
  separately, mapping body declared separately, `.all()` called at
  use-site. Four named entities + a call.
- **`^Class(args)`** *(constructor)*: same four facets, **collapsed
  inline**. Class declared elsewhere; store ("one in-memory row") +
  mapping (the args) + `.all()` (yield the row) all live in one
  expression. No new model elements are allocated; the four facets
  are just co-located in the TypedSpec node.
- **Pipeline `source->extend(~p:e)`**: composes an existing
  producer, layering bindings on top without changing the underlying
  class or store.

Same conceptual shape, different fragmentation. This is the **mental
model**, not an implementation prescription — we don't materialize a
synthetic class/store/mapping for every `^Class()`. The point is
that producers are conceptually equivalent and admit a uniform
"what does property P resolve to" query.

#### The view itself

Every TypedSpec node that produces a class-typed value answers one
question: **`bindings() : Map<propertyName → PropertyBinding>`**.
That's the entire interface consumers care about. `PropertyBinding`
is one of:

- **Scalar**: a column reference (`$row.col`) or computed expression.
- **Association**: a navigation (target class + join condition).
- **Embedded**: a nested `PropertyBindings` for an embedded
  sub-producer.

Each producer node knows how to compute its own bindings from its
structure:

| Producer | How `bindings()` is computed |
|---|---|
| Named function returning `Relation<C>` (mapping function) | Function body's structural view, memoized at the model level via `ModelContext.indexFor(funcRef)`. |
| Constructor `^Class(prop1=e1, ...)` | The constructor's args. |
| Pipeline `source->extend(~p:e)` | `source.bindings() ⊕ {p ↦ scalar(e)}`. |
| Pipeline `source->filter(...)` | `source.bindings()` (unchanged). |
| Function call `f(...)` returning class-typed | Resolves to `f`'s body view, which is one of the above. |

The memoization for named functions is a perf detail (re-walking a
mapping body for every call site is wasteful), not a separate
concept. From the consumer's perspective `node.bindings()` is
one call; whether it hits a cache or recomputes structurally is
invisible.

#### Consumers

Every consumer that needs "what does property P resolve to here?"
reads from `bindings()`:

- `TypeChecker`: validates that every property access targets a
  binding in the source's view (otherwise §1.10 error).
- `MappingResolverV2`: navigation-hop resolution; leaf physical-column
  resolution; constructor property resolution.
- Demand pruning (in `Lowerer`): which bindings to lower for a given
  column subset.

No consumer walks function bodies by hand to find extend cols by
alias. No consumer pattern-matches "is this a `^Class()`?" to take a
different branch from "is this a mapping function call?". The
abstraction makes them identical.

Borrowed from Calcite's `RelMetadataQuery` (metadata as queries
against a logical plan; computed structurally per node, memoized
where reuse warrants it) and Catalyst's `AttributeMap` (cached
resolved-attribute lookups).

### 1.9 Mapping DSL is sugar `[← Scala for-comprehensions, C# query syntax]`

The mapping DSL (`Mapping Person : Relational { ... }`) is a
parser-level convenience that desugars to function definitions. It
exists for user concision; it has **no presence** in any IR after
`MappingNormalizer`. Users who prefer to write functions directly
get identical semantics. There is no privileged "mapping" form
anywhere downstream of Normalizer.

This is analogous to how Scala for-comprehensions desugar to
`flatMap`/`map`/`filter` calls, or how C#'s LINQ query syntax
desugars to method calls. The sugar is a syntactic affordance; the
language semantics are defined on the desugared form.

### 1.10 No implicit semantics

Every queryable property/column is the product of an **explicit
binding** somewhere in the user's source. The compiler does not
synthesize, default, or infer property bindings. The rule applies
**uniformly across all producers** (§1.7) — querying a property `P`
on a producer is well-formed iff `P` is in the producer's
`PropertyBindings` (§1.8).

Concretely:

- A mapping function whose body does not bind property `P` makes `P`
  non-queryable on instances it produces. Querying `P` is a
  compile-time error, not a silent identity default.
- A constructor `^Person(firstName='X')` makes only `firstName`
  queryable on the resulting instance. `^Person(firstName='X').age`
  is a **compile-time error** — `age` is not in the constructor's
  bindings. (If the user wants `age` defaulted, they write
  `^Person(firstName='X', age=42)` or `^Person(firstName='X', age=[])`
  explicitly.)

  **Today's code violates this invariant**: `MappingResolverV2.computeIndex`
  has a `TypedNewInstance` arm that calls `identityIndexFromClass(ni.className())`,
  building an identity map from *all* the class's declared properties
  rather than from the constructor's args. The current behavior is
  precisely the "implicit identity default" this invariant outlaws.
  A `^Person(firstName='X')` instance today silently exposes `age`,
  `lastName`, etc. as queryable; under §1.10 it would not.
- An association is queryable only if the producer explicitly binds
  it (mapping function body bound via `~assoc: traverse(...)`, or
  constructor arg `^Class(assoc=$other)`). Unbound associations are
  not navigable.
- Identity behavior, if wanted, is **written explicitly**: in a
  mapping function, `~P: $row.P`; in a constructor, `P=$source.P`.

This eliminates an entire class of footguns: silent column-name
coincidence, missing mapping for a renamed source column,
property-name typos in queries that resolve to identity defaults,
partially-constructed instances whose unset properties silently
return empty. Every binding is explicit and audit-trailable.

#### 1.10.1 Constructors do not need a wrapping mapping function

A `^Class(args)` expression is **conceptually equivalent to** a
one-row `(class + store + mapping + `.all()`)` composite (§1.8) —
collapsed inline. We do **not** materialize that composite as four
synthetic model elements: no synth class, no synth store, no synth
mapping function, no synth FQN. The TypedSpec node IS the composite;
all four facets are co-located in its structure.

Concretely:
- The class is already declared (`^Class` references it by FQN).
- The "store" is implicit: a single in-memory row.
- The "mapping" is the args themselves — that's the constructor's
  `PropertyBindings`.
- The "`.all()` query" is implicit: yield the row.

Consumers query the constructor's bindings through the same
`bindings()` interface as a mapping function call. No special
case in `TypeChecker`, `MappingResolverV2`, or `Lowerer`. The
constructor is fully a first-class producer.

Why not synth-mapping-function-per-`^Class()`? Because that would
require allocating per-call-site FQNs in the model (one per
`^Class()` syntactic occurrence), bloating `ModelContext` with
per-query ephemera, and departing from Pure's source semantics
(where `^Class()` is an expression, not a definition). Production
compilers (Calcite, Catalyst, rustc) consistently keep
expression-level semantics structurally on the expression node,
reserving model-level memoization for named definitions only.

---

## 2. Non-goals

These are deliberately *not* supported. Stating them avoids
accidentally bending the design toward them.

### 2.1 No incremental compilation

A change to any input element triggers full re-evaluation of every
dependent element. We do **not** maintain a "what changed" diff and
recompute selectively.

Rationale: every incremental compiler in production
(rustc's incremental cache, scalac's TASTy, javac's `-incremental`)
has had recurring staleness bugs. The cache-invalidation logic is
where the bugs live, and the wins shrink the moment a change crosses
a module boundary. We trade rebuild speed for correctness.

### 2.2 No invalidation-based caching

If we cache, we cache **content-addressed**. The cache key is a hash
of the inputs; the cache value is the output. Cache entries are never
"updated" or "invalidated" — a content change is a different key, a
different entry. The cache has no failure mode.

This is how Bazel, Buck, and Nix work. It composes naturally with
cross-project: cache by `(project_content_hash, element_fqn) → typed_element`.

### 2.3 No mutable shared state across passes

A pass takes immutable inputs and produces immutable outputs. Two
passes never write to the same data structure. `MappingNormalizer`
does **not** populate a `NormalizedMapping` that `TypeChecker`
later reads from and writes back to.

If a pass needs to maintain state during its own walk (e.g. MR2's
`navs` accumulator), that state is **stack-local** to the pass — not
exposed to other passes.

---

## 3. `ModelContext` — the read-only model view `[← rustc TyCtxt, Bazel BuildSettings]`

Today, `PureModelBuilder` *is* `ModelContext` — the same mutable
object serves the parser, the type checker, and downstream passes.
This is a problem because mutation can leak across pass boundaries,
and the type doesn't distinguish "cheaply available" from "requires
type-checking" data.

The invariant is: **one read-only `ModelContext` type, with field
population strategies tiered by cost**.

#### Tiered fields

| Tier | Examples | When populated |
|---|---|---|
| **Cheap (eager)** | `kindOf(fqn)`, `exists(fqn)`, package layout, FQN ↔ stable id | At construction, directly from parser output. No type-checking required. |
| **Medium (eager-or-lazy)** | Class property signatures, function signatures, association ends | Populated as part of `PureModelBuilder` (§5.6). Cheap enough to be eager in practice. |
| **Expensive (lazy)** | Typed function bodies, `PropertyBindings` for mapping functions, fully-resolved expressions | Populated on first access. Triggers TypeChecker/`indexFor` machinery. |

NameResolver only reads cheap-tier fields. TypeChecker and
MappingResolverV2 may trigger expensive-tier fields. The same
`ModelContext` instance threads through all passes — no Promotion
or Skeleton-vs-Context split.

#### Read-only discipline

- No mutator method on `ModelContext`. Mutation lives entirely on
  `PureModelBuilder` (which produces a `ModelContext` snapshot and
  is then dropped).
- Lazy fields are `compute-then-cache` under a thread-safe gate;
  once computed, they're immutable for the rest of the context's
  lifetime.
- A pass receiving a `ModelContext` cannot distinguish "this field
  was populated eagerly" from "this field was populated by a
  previous lazy access." The result is the same.

#### Why one type, not two

An earlier draft of this doc proposed splitting `ModelSkeleton`
(cheap) from `ModelContext` (full). On reflection, the chicken-and-egg
that motivated the split disappears once you allow lazy fields:
NameResolver reads only what's eagerly populated; TypeChecker reads
the lazy stuff and triggers its computation. Two types would just
add promotion-bookkeeping with no extra correctness guarantee.
rustc takes the same shape: it's all `TyCtxt` with lazy queries;
`CrateInfo` is internal to crate loading, not a separate API tier.

#### API surface (illustrative)

```
interface ModelContext {
  // cheap tier
  Kind kindOf(Fqn);             // Class | Function | Association | Store | NotFound
  boolean exists(Fqn);
  StableId idFor(Fqn);          // SymbolTable interning

  // medium tier
  CompiledClass classFor(Fqn);
  CompiledFunction functionFor(Fqn);   // signature; body may be lazy
  CompiledAssociation associationFor(Fqn);

  // expensive tier (lazy)
  TypedSpec bodyOf(Fqn funcRef);
  PropertyBindings indexFor(Fqn funcRef);  // §1.8 memoization
}
```

**Invariant**: every method is referentially transparent for a
given `ModelContext` instance. Two calls with the same FQN return
`.equals()`-equal results.

---

## 4. Caching `[← Bazel content-addressed actions, rustc query system]`

Caching is content-addressed and lossless.

```
cache_key = blake3(project_source_hash || dependency_hashes || pass_id || element_fqn)
cache_val = serialized typed element
```

Properties:
- **No invalidation logic.** A source change is a new key.
- **Cross-project transparent.** Dependency hashes propagate.
- **Per-pass cacheable.** Each pass can independently cache its outputs:
  parser output, type-check output, mapping-resolve output, MIR, SQL.
- **No correctness dependency.** Wiping the cache must produce
  bit-identical output to a cache hit. The cache is a speed
  optimization, never a correctness primitive.

The first implementation may not have any caching. The invariant is
that *if and when we add caching, it must be content-addressed*. No
"smart" invalidation, ever.

---

## 5. Per-pass contracts

Each section below declares one pass's contract:

- **INPUT SHAPE** — what the pass receives.
- **OUTPUT SHAPE** — what the pass produces.
- **INVARIANTS** — what MUST be true about the output.
- **NON-INVARIANTS** — what is explicitly NOT guaranteed.
- **LESSONS** — borrowed ideas relevant to this pass, inline.

### 5.1 Lexer (`PureLexer2`)

- **INPUT**: source text (`String` or stream).
- **OUTPUT**: token stream (`List<Token>` or iterator).
- **INVARIANTS**:
  - Every token carries source location (offset, length, line, col).
  - Token kinds are a closed enum (`TokenType`).
  - Whitespace and comments are either dropped or carried as trivia
    (decision: dropped for now; revisit if formatter is built).
- **NON-INVARIANTS**:
  - No semantic validation. `42abc` lexes as a number token followed
    by an identifier; semantic rejection is the parser's job.

### 5.2 Model Parser (`PureModelParser`)

- **INPUT**: token stream from a `.pure` model file.
- **OUTPUT**: `ParseResult` containing untyped definitions
  (class defs, association defs, mapping defs, function defs).
- **INVARIANTS**:
  - Every parsed definition carries source location.
  - Every name is a string at this stage; resolution is `NameResolver`'s
    job. The parser does not consult any model.
  - The output is a forest, not a graph: no cross-references between
    definitions are followed.
- **NON-INVARIANTS**:
  - No type information.
  - No name resolution. `Person` and `app::people::Person` may both
    appear in the output as raw strings.
- **LESSONS** `[← Rust syn]`: parser produces untyped AST; semantic
  passes operate on it. The parser does not know what a `Person` is.

### 5.3 Query Parser (`PureQueryParser`)

- **INPUT**: token stream from a Pure query (function body, expression).
- **OUTPUT**: untyped `ValueSpecification` AST.
- **INVARIANTS**:
  - Every node carries source location.
  - Function calls carry the called name as a string; overload
    resolution is `TypeChecker`'s job.
  - Variable references are raw names; binding resolution is also
    `TypeChecker`'s job.
- **NON-INVARIANTS**:
  - No types, no overload selection, no scope resolution.
- **LESSONS**: same as model parser.

### 5.4 NameResolver

- **INPUT**: untyped AST + `ModelContext` (only cheap-tier fields
  read — `kindOf`, `exists`).
- **OUTPUT**: AST with every name disambiguated to an FQN. Imports
  resolved, package-relative names canonicalized.
- **INVARIANTS**:
  - After NameResolver, every name in the AST is an FQN
    (`app::people::Person`, not `Person`).
  - Every FQN exists in `ModelContext` — references to undefined
    elements are rejected here, not deferred.
  - Resolution is one-pass: the resolver does not type-check to
    disambiguate. Only `ModelContext.kindOf(fqn)` is consulted.
- **NON-INVARIANTS**:
  - No types.
  - No overload selection (overload selection requires types,
    deferred to TypeChecker).
- **LESSONS** `[← rustc resolve crate]`: name resolution is its own
  pass, distinct from type-checking. The Rust resolver runs before
  type inference and produces a `Resolutions` table.

### 5.5 MappingNormalizer

> **Pipeline position**: between `NameResolver` (§5.4) and
> `PureModelBuilder` (§5.6). Pure AST → AST pass. No type
> information consumed or produced. No `ModelContext` access (none
> exists yet).

- **INPUT**: name-resolved `ParseResult` containing `MappingDefinition`,
  `FunctionDefinition`, and other `PackageableElement`s.
- **OUTPUT**: name-resolved `ParseResult` where every `MappingDefinition`
  has been **replaced** by a `FunctionDefinition`. The `definitions`
  list contains zero `MappingDefinition`s after this pass.
- **INVARIANTS**:
  - Pure structural desugar (§1.9): no type info, no compiled model
    consulted.
  - User-written mapping-shaped functions and DSL-desugared functions
    are **indistinguishable** in the output — same code path through
    `PureModelBuilder` and `TypeChecker`.
  - Multi-mapping `extends` is desugared as **function composition**:
    `child(): C[*] { parent()->extend(...) }`. Inheritance is a
    regular function call, resolved by the standard mechanism.
  - No `NormalizedMapping` sidecar exists, ever.
  - No "synth function" concept exists downstream — they're plain
    functions identified only by their return type
    (`Relation<C>`).
- **NON-INVARIANTS**:
  - Does **not** emit identity-property defaults. Per §1.10, unbound
    properties are non-queryable; users write `~P: $row.P` if they
    want identity.
  - Does **not** synthesize association extends. Associations must
    be explicitly bound in source.
  - Does **not** choose entry strategies (`Lowerer` concern).
  - Does **not** prune dead extends (`Lowerer` demand-pruning concern).

#### Why this position solves the chicken-and-egg

An earlier draft placed Normalizer post-`PureModelBuilder`/post-
`TypeChecker` so it could emit *typed* extends directly. That
violates the dependency order: Normalizer would need typed bodies
that only TypeChecker produces. By making Normalizer pure-syntactic
and pre-builder, the typed-extend question goes away — TypeChecker
type-checks the desugared function bodies uniformly, and the
property index (§1.8) reads typed bindings from those bodies.

It also collapses the **dual element-compile path**: today,
`PureModelBuilder` has separate logic for `MappingDefinition` vs
`FunctionDefinition`. Under this invariant, Normalizer eliminates
`MappingDefinition` from the pipeline; `PureModelBuilder` sees only
`FunctionDefinition`s.

- **LESSONS** `[← Scala desugaring of for-comprehensions]`: the
  surface syntax is sugar; the desugared form is what the type
  system operates on. `[← C# LINQ query syntax → method calls]`:
  same pattern — surface sugar, method-call desugaring, uniform
  downstream.

### 5.6 Element Compiler (`PureModelBuilder`)

Includes `RelationalMappingConverter` (sub-pass) and `SymbolTable`.

- **INPUT**: post-Normalizer `ParseResult` + `ModelContext` (cheap
  tier). Contains no `MappingDefinition`s.
- **OUTPUT**: `Compiled*` model elements (`CompiledClass`,
  `CompiledFunction`, `CompiledAssociation`, etc.) forming a
  `ModelContext`-grade view.
- **INVARIANTS**:
  - Every property has a typed reference (not a string) for its type.
  - Every function has a typed signature (parameter types,
    return type) — bodies may still be untyped (deferred to
    TypeChecker).
  - Every association has both ends resolved to class references.
  - `SymbolTable` has interned every FQN to a stable integer id.
    Two `Compiled*` elements referring to the same FQN share the
    same id.
  - **Single element-compile path**: `FunctionDefinition →
    CompiledFunction`. There is no `CompiledMapping`. Mapping
    semantics are carried entirely by functions whose return type is
    `Relation<C>`.
  - `CompiledFunction`s with return type `Relation<C>` are eligible
    for `PropertyBindings` memoization via `indexFor(funcRef)`
    (§1.8); not populated by this pass.
- **NON-INVARIANTS**:
  - Function bodies are not yet type-checked (TypeChecker's job).
  - `PropertyBindings` memoization is not yet populated (lazy,
    post-TypeChecker).
- **LESSONS** `[← Calcite RelDataType, Trino Type registry]`: the
  type system is its own resolved structure, populated before
  expression-level type checking.

### 5.7 TypeChecker

Includes `checkers/` subsystem (per-native dispatch),
`BuiltinRegistry`, `Pure.java`, `TypeCheckEnv`.

- **INPUT**: name-resolved query AST + `ModelContext`.
- **OUTPUT**: `TypedSpec` HIR (typed, fully resolved).
- **INVARIANTS**:
  - Every `TypedSpec` node has a populated `ExpressionType`.
  - Every variable reference is a `TypedVariable` carrying its
    binding's type and (for class-typed bindings) class reference.
  - Every property access is a **resolved** `TypedPropertyAccess`
    carrying typed hop references — no `List<String>` paths survive.
  - Every function call resolves to a typed `NativeFunctionDef` or
    `TypedReference<PureFunction>`. No string-typed function dispatch.
  - Every overload is selected; no `AppliedFunction` carries a raw
    name without a resolved target.
  - Every multiplicity is checked.
  - DSL-desugared mapping functions and user-written functions are
    type-checked via the **same code path**. TypeChecker does not
    know "mapping function" as a category; it sees only function
    bodies distinguished by return type (§1.7).
  - Every property access is validated against the source's
    `PropertyBindings` (§1.8). Accessing a property not in the
    view is a compile-time error (§1.10) — applies uniformly whether
    the source is a mapping function call, a constructor `^Class(...)`,
    or a pipeline expression.
  - Constructor expressions (`^Class(args)`) are typed nodes
    (`TypedNewInstance`) carrying their args as a
    `Map<propName, TypedSpec>`. Their `PropertyBindings` is
    structurally derivable from the args; no separate computation
    needed.
- **NON-INVARIANTS**:
  - `TypedGetAll` may still appear in the output. Its inlining is
    `MappingResolverV2`'s job.
  - `TypedUserCall` may still appear. Its inlining is also MR2's job.
  - The model-level memoization of `PropertyBindings` for named
    class-relation functions is not populated here. It's built
    lazily on first call to `ModelContext.indexFor(funcRef)`
    (§1.8). Inline producers' bindings are computed structurally
    on demand; consumers MAY keep a pass-local cache (today's
    `MappingResolverV2.indexCache : IdentityHashMap<TypedSpec, RowIndex>`
    is exactly this), but there is no model-level cache for them.
- **LESSONS** `[← Catalyst Analyzer]`: name+type resolution is a
  rule-based rewrite over the AST, producing a tree where every
  expression has a fully-resolved `dataType` and `nullable`.
  `[← rustc HIR]`: the typed HIR is a different node hierarchy from
  the parsed AST; resolution is a transformation, not an annotation.

### 5.8 MappingResolverV2

Subsumes `UserCallInliner` (today a separate pass; should be one of
MR2's rewrite rules). The dead `MappingResolver` V1 and
`PropertyAccessPopulator` should be deleted as part of this pass's
audit.

- **INPUT**: typed HIR with `TypedGetAll`, `TypedUserCall`, and
  path-bearing property accesses.
- **OUTPUT**: typed HIR with **none** of those.
- **INVARIANTS**:
  - No `TypedGetAll` remains.
  - No `TypedUserCall` remains.
  - No path-bearing `TypedPropertyAccess` remains — every property
    access is at a leaf, with its physical column reference resolved.
  - Every association traversal is an explicit `TypedJoin` or
    (for to-many) `TypedFlatten`/`TypedJoin` with appropriate
    multiplicity semantics.
  - Class-typed roots are wrapped in `TypedSerializeImplicit` with
    a resolved leaf tree.
  - Property/association resolution reads from the source
    expression's `PropertyBindings` (§1.8). For named class-relation
    functions the view comes from `ModelContext.indexFor(funcRef)`;
    for inline producers (constructors, pipeline expressions) the view
    is computed structurally. MR2 does not walk function bodies to
    find extend cols by alias.
  - Querying an unbound property (one not in the source's
    `PropertyBindings`) is a compile-time error per §1.10 —
    surfaced by TypeChecker; MR2 only sees already-validated trees.
  - Constructor expressions (`^Class(args)`) are handled uniformly
    with mapping-function calls. There is no separate code path for
    "instance from constructor" vs "instance from mapping". Both flow
    through the `bindings()` abstraction.
- **NON-INVARIANTS**:
  - The HIR may still contain Pure-shaped operators
    (`TypedExtend`, `TypedFilter`, etc.); lowering to relational
    primitives is the `Lowerer`'s job.
- **LESSONS** `[← Catalyst Analyzer rules]`: this pass should be
  refactored into a composition of small, named rewrite rules
  (illustrative split: `InlineUserCalls`, `InlineClassFetches`,
  `ResolveAssociationPaths`, `ExpandImplicitSerialize`), each with
  a sharp local contract. (Today MR2 is a single visitor; these
  rule names are aspirational, not current code.)
  `[← LINQ-to-SQL expression visitor]`: traversal is a structural
  walk; rewrite rules are local pattern matches.

### 5.9 Lowerer (HIR → MIR)

Defined in `docs/pipeline-architecture.md` §2. Summary:

- **INPUT**: resolved typed HIR.
- **OUTPUT**: MIR (`SqlExpr`, `SqlRelation`, `SqlAggregate`).
- **INVARIANTS** (per pipeline-architecture.md):
  - Closed sealed MIR; pattern-match exhaustiveness enforced.
  - No String fields encode dispatch (single carve-out: `Cast`'s
    Pure type name).
  - No HIR nodes leak into MIR.
- **NON-INVARIANTS**:
  - SQL syntax (dialect's job).

### 5.10 SQLDialect (MIR → SQL)

Defined in `docs/pipeline-architecture.md` §2.

- **INPUT**: MIR.
- **OUTPUT**: SQL string.
- **INVARIANTS**:
  - No HIR or model lookups; the dialect sees only MIR.
  - `render(SqlExpr)` is a switch *expression* over the sealed MIR.

---

## 6. AST shape invariants — the `typed/` hierarchy

The HIR is the central data structure that ties the pipeline together.
This section pins down its shape.

### 6.1 Sealed and exhaustive

`TypedSpec` is `sealed` with a `permits` clause naming every variant.
Adding a new node without updating consumers is a javac error.

### 6.2 Every node carries `ExpressionType`

`info().type()` is populated for every node. There is no node where
the type is unknown.

### 6.3 No string-typed dispatch

Function calls, property accesses, and references all carry resolved
typed references — never string names that downstream code dispatches on.

### 6.4 Resolved vs unresolved variants

For every "resolution" pass (NameResolver, TypeChecker,
MappingResolverV2), if the pass turns an unresolved shape into a
resolved one, the input and output are *distinct types* (or at
minimum, distinct cases of a sealed hierarchy).

Concretely:

```java
// Today (wrong): one type, two states distinguished by flags.
record TypedPropertyAccess(
    TypedSpec source, String property,
    Optional<List<String>> associationPath,
    Optional<String> physicalColumn,
    ExpressionType info) implements TypedSpec {}

// Invariant (right): two types, state in the type.
sealed interface TypedPropertyAccess extends TypedSpec
        permits TypedPropertyAccess.Path, TypedPropertyAccess.Column {
    record Path(TypedSpec source, List<HopRef> hops, ExpressionType info)
            implements TypedPropertyAccess {}
    record Column(TypedSpec source, ColumnRef column, ExpressionType info)
            implements TypedPropertyAccess {}
}
```

After `MappingResolverV2`, every `TypedPropertyAccess` in the tree
is `TypedPropertyAccess.Column`. The `Path` variant is unrepresentable
in the resolver's output type if we choose to refine it
(post-MR2 HIR uses `sealed interface ResolvedHir permits ...` which
omits `Path`).

### 6.5 `TypedReference<T>` for cross-element references

Every reference to a model element (`PureClass`, `PureFunction`,
`Association`, `Mapping`, `Store`) is a `TypedReference<T>`, never an
embedded element instance. Resolution is via
`ModelContext.resolve(ref)`.

This is what makes `TypedSpec` trees comparable, serializable, and
cheap to transmit.

### 6.6 No path-bearing `TypedPropertyAccess` after MR2

This is the specific invariant that motivated this whole document.
After `MappingResolverV2`, the only property accesses in the tree are
leaf accesses on resolved row sources. Every association traversal
has been lifted into an explicit `TypedJoin` (or `TypedFlatten`).

### 6.7 Class-instance producers and their bindings

Every TypedSpec node whose `info().type()` is class-typed (or
`Relation<C>`) is a **producer** carrying a `PropertyBindings`
(§1.8). The known producer kinds are:

| Node kind (illustrative) | How `bindings()` is computed |
|---|---|
| `TypedUserCall` whose callee returns `Relation<C>` | `ModelContext.indexFor(callee)` (memoized). |
| `TypedNewInstance(className, values)` (the `^Class(...)` constructor) | The values map itself, structurally. |
| `TypedExtend(source, cols...)` | `source.bindings() ⊕ cols`. |
| `TypedFilter(source, pred)` | `source.bindings()`. |
| `TypedJoin(left, right, ...)` | Combined per join semantics. |
| `TypedSerializeImplicit(source, tree)` | `source.bindings()` projected to the tree's leaves. |
| `TypedVariable(binding)` whose binding is class-typed | The binding's defining producer's `bindings()`. |

Property accesses (`TypedPropertyAccess`) are validated against
their source's bindings by TypeChecker (§5.7). Constructors are
**first-class producers** — they are not desugared to function
calls, do not require a wrapping mapping function, and participate
in `PropertyBindings` directly via §1.8.

---

## 7. MIR shape invariants

See `docs/pipeline-architecture.md` §3. Summary in three rules:

1. Every operation is its own record (no `FunctionCall(String, args)`).
2. No String fields encode dispatch (single carve-out: `Cast`'s
   Pure type name).
3. Pattern-match exhaustiveness is the test (sealed + `permits`).

---

## 8. Lessons borrowed (consolidated)

For convenience, the inline `[← X]` markers across this document are
collected here. Each is a one-line note on what we steal.

- **rustc HIR/MIR transition**: passes transform AST type, don't flag.
  Resolved state is a different type, not a `boolean resolved`.
- **rustc query system**: per-element on-demand computation, memoized.
  Lazy ↔ eager equivalence is an emergent property of the query
  abstraction.
- **rustc `DefId`**: cross-element references are interned ids, not
  pointers. Stable across compilation runs (with content-addressed
  caching).
- **rustc resolve crate**: name resolution is its own pass, separate
  from type inference.
- **Spark Catalyst Analyzer**: name+type resolution as rule-based
  rewrites with a `resolved` invariant on every plan node.
- **Spark Catalyst rules composition**: complex passes (like our
  MR2) are compositions of small named rules with sharp local
  contracts.
- **Calcite RelNode + RelDataType**: the relational algebra is its
  own typed IR distinct from the expression IR; row shape is
  first-class.
- **Calcite RelTraitSet**: physical properties (sort order, hash
  distribution, etc.) attach to relational nodes as typed traits.
  Relevant when our MIR grows physical optimization.
- **Trino SymbolReference**: every column in the planner is referenced
  by symbol id, never by string name.
- **Trino SymbolAllocator**: the symbol table is centralized and owned
  by the planner; passes obtain symbols from it, never invent strings.
- **Bazel content-addressed actions**: cache by hash of inputs; never
  invalidate; cross-project transparent.
- **Bazel build settings vs configured target**: same source can be
  consumed at multiple "configurations" without conflating them.
  Inspiration for `ModelContext` as a single immutable view per
  configuration.
- **Dotty phases**: explicit phase ordering with `runsAfter` /
  `runsBefore`. Probably overkill for us today; worth knowing.
- **LINQ deferred execution**: the expression tree is built without
  side effects; execution happens when the query is materialized.
  Inspiration for Interpreted++.
- **LINQ-to-SQL expression visitor**: small, structural translator
  from typed expression tree to SQL. Model for our HIR-to-MIR
  lowering.
- **SQLGlot Scope abstraction**: per-query scope tree with parent-chain
  name resolution. Cleaner than threading `envClass` through scope.
- **DuckDB bind/optimize/execute**: clean separation of name binding
  from logical plan from physical plan. Mirrors our parser /
  TypeChecker / Lowerer split.
- **syn (Rust)**: parser produces untyped AST; semantic passes
  operate on it. Parser does not consult the model.
- **Lisp/Scheme**: program is data + functions, no privileged forms.
  Foundational for §1.7 "everything is a function."
- **Calcite `RelMetadataQuery`**: derived metadata over a logical
  plan, lazily computed; multiple metadata kinds (row count,
  distinct keys, predicates) coexist as queries. Direct model for
  `PropertyBindings`.
- **Catalyst `AttributeMap`**: cached resolved-attribute lookups
  over a logical plan node. Same pattern as our index.
- **Scala for-comprehensions**: surface syntax desugars to
  `flatMap`/`map`/`filter` calls; the desugared form is what the
  type system operates on. Model for our mapping-DSL desugaring.
- **C# LINQ query syntax**: surface query syntax desugars to method
  calls. Same pattern — sugar in, methods out, uniform downstream.

---

## 9. Audit process

This document declares ideal state. The current code does not yet
satisfy every invariant. The path from here:

### 9.1 Per-pass gap docs

For each pass, when we sit down to audit it, we create
`docs/audits/<pass>-gap.md` capturing:

- **Current shape** — what the pass actually produces today.
- **Gap** — concrete deviations from this document's invariants.
- **Migration plan** — ordered steps to close each gap.
- **Test coverage** — which tests verify each invariant.

The gap docs are the **only** place "current state" is described.
This document remains pure ideal state. When a gap is closed, the
gap doc is updated to note the closure (or the gap doc is archived).

### 9.2 Confidence-driven audit order

We audit passes in the order that gives us the most confidence per
unit of effort, **not** strict pipeline order. This document's
forward declaration lets us start anywhere.

Provisional order (revisable as we learn):

1. **AST shape (`typed/` hierarchy)** — pin down resolved vs unresolved
   variants. Most string-leakage lives here. Highest leverage.
2. **TypeChecker** — populate the new shapes.
3. **MappingNormalizer** — relocate to pre-`PureModelBuilder`; rewrite
   as pure AST→AST desugar (DSL → `FunctionDefinition`); shed
   identity-default / association-synthesis responsibilities; kill
   the `NormalizedMapping` sidecar.
4. **`PropertyBindings` (§1.8)** — implement as a `ModelContext`
   service. Replaces today's `RowIndex`/`buildIndex` pattern with a
   model-level memoized lookup.
5. **MappingResolverV2** — by now much smaller; finish trim, absorb
   `UserCallInliner`, delete V1 + `PropertyAccessPopulator`. Read
   from `PropertyBindings` instead of walking bodies.
6. **Backend (Lowerer / SQLDialect)** — verify against the cleaner
   HIR. Probably mostly mechanical.
7. **Parser / NameResolver / ModelBuilder** — enforce read-only
   `ModelContext`; tier its fields (cheap eager, expensive lazy);
   delete any leftover mutable-context paths. Sets up cross-project
   work.

### 9.3 Updating this document

When an invariant changes (rare; this doc is supposed to be stable),
the change must:

- Be justified in the commit message with a concrete reason.
- Update every per-pass section affected.
- Update or archive every gap doc affected.
- Be accompanied by a code change that brings the code into
  conformance with the new invariant in the same PR (or a follow-up
  PR explicitly tracked).

---

## 10. What this document is not

- **Not a tutorial.** Read `docs/pipeline-architecture.md` and
  `docs/frontend-architecture.md` for narrative.
- **Not a description of current state.** Read the gap docs.
- **Not a list of rules to follow blindly.** If an invariant seems
  wrong for a real situation, the right move is to question the
  invariant in the doc, not to silently violate it in the code.
