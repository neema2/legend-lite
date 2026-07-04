# Core Phase F — Typed Elements (`compiler/element/`) — Design

> **Scope:** `legend-lite/core` Phase F (`ElementCompiler` + `compiler/element/` +
> `ModelContext`). Read `core/README.md` first — this doc refines its Phase-F
> blueprint (layout lines ~143–172, invariants 7 & 11). It does **not** touch
> `engine/`; engine is cited only as a reference design.
>
> **Status:** Proposed, pre-implementation. F sits downstream of D and E, which are
> **done** (`core/README.md` Status): D = `parser/ImportResolver` + `ImportScope`
> (116 tests); E = `normalizer/MappingNormalizer` (86 tests). This doc pins the
> shapes so they don't churn when coding starts.

---

## TL;DR

- The Element Compiler **consumes** parser output (`parser.element.*` def
  records, `parser.spec.ValueSpecification` bodies) as **input** — this is what
  every real compiler does. Its **output** (`Typed*`) is a separate, pure-data,
  FQN-referenced, serializable-by-construction model.
- **Typed signatures own their value types.** `compiler/element/type/` defines
  its own `Type` **and** `Multiplicity`. We do **not** reuse `parser.Multiplicity`
  in typed signatures — that would drag `com.legend.parser.*` into every
  downstream consumer. Convert at the `ElementCompiler` boundary.
- **`ModelContext` is an interface**, not a class. `findClass(fqn)` is the single
  structural-lookup choke point; making it lazy / dependency-backed later
  requires zero consumer changes. This is the door that keeps cross-project
  element loading from becoming a rewrite.
- **Every Pure body is a function.** Function bodies, service queries, derived
  properties, constraints, and mapping transforms all desugar (in E) to
  synthesized `Function`s. Elements hold **signatures + FQN refs**, never bodies.
  Consequence: the **only** compiled body is a function's `TypedSpec` (G's output),
  the **only** body resolver is `findFunction`, and there are **no per-element
  compiled-body side tables**. See **F-6** for this decision; **F-1** for the
  residual value-spec AST dependency (now confined to `TypedFunction`).

---

## 1. Where F sits, and the engine layering lesson

Pipeline (`core/README.md`): `… D NameResolver → E MappingNormalizer → F
ElementCompiler → G SpecCompiler …`. F turns *resolved, normalized def records*
into the *typed model* + a `ModelContext` the rest of the pipeline queries.

The engine (`com.gs.legend`) keeps **three** layers (`docs/PHASE_B_COMPILED_ELEMENTS.md` glossary):

| Layer | Engine | Role |
|---|---|---|
| **def record** | `model.def.ClassDefinition`, … | parser output; structural source |
| **semantic model** | `model.m3.PureClass`, `Property`, `Type`, `PureFunction` | what `TypeChecker`/`PlanGenerator` operate on; exposed via `ModelContext` |
| **compiled artifact** | `compiled.CompiledClass`, … (`CompiledElement`) | pure-data, no live refs, no cycles → serialized to `.legend` for cross-project deps |

**Two independent collapses. Both real, neither for-collapse's-sake.**

**Collapse 1 — signatures: one pure-data record, no `m3` vs `compiled` split.**
The engine keeps a separate in-memory `PureClass` *and* a serializable
`CompiledClass` for the same class structure. core/ keeps **one**, because
`m3.PureClass` is *already* pure-data + FQN-ref — the duplication buys nothing for
*signatures*. So one `TypedClass`/`TypedEnum`/… signature record serves the
in-memory model and the on-disk artifact.

**Collapse 2 — bodies: every Pure body is a `Function`, so there is exactly one
body-bearing element (`Function`) and one compiled-body form (a `Function`'s
`TypedSpec`).**
This is the decision in **F-6** (option A, externalize). Phase E already desugars
mappings into synthesized `Function`s; F-6 finishes that move for the other three
body sites:

| Pure body site | Desugars (in E) to | Owning element keeps |
|---|---|---|
| function body | (already a `Function`) | — |
| service query | a synthesized `Function` | FQN ref |
| derived property `fullName(){…}:T[m]` | `Owner::fullName(this:Owner[1],…):T[m]` | the **signature** (name/type/mult/params) + FQN ref |
| constraint `c: <bool over $this>` | `Owner::$constraint$c(this:Owner[1]):Boolean[1]` | constraint **metadata** (name, enforcement, message-fn FQN) + FQN ref |
| mapping transform / `~filter` | synthesized `Function` (already done in E) | FQN ref |

So **no element other than `Function` carries a `ValueSpecification`.** Classes hold
property/derived-property *signatures* + constraint *metadata* + FQN refs; services
hold config + an FQN ref. Bodies live in the global function registry, addressed by
owner-qualified FQN, resolved by the **same** `findFunction` as user functions.

**Why this is the no-side-car design (the point):** because every compiled body is
a `TypedSpec` keyed by function FQN in the **one** FQN → `TypedSpec` table, there is
**no per-class / per-member compiled-body side structure** — the thing that "got
out of hand" before. The alternative (member-scoped bodies, engine's
`qualifiedProperties`) keeps the function *shape* but forces the compiled body into
a `(class, name)`-keyed side table; F-6 rejects it for exactly that reason.

**The body's two representations now apply only to `Function`:**

- **In-memory (producer):** the **parsed, un-type-checked `ValueSpecification`** —
  input to G (invariant 7). This is real AST: parsed by C and name-resolved by D
  (FQNs already stamped); only *type-checking* is pending. It is **not** raw text
  (that representation was eliminated when D-1 closed) and **not** "unresolved"
  (D ran). `core` already has it post-parse. **Present only for functions compiled
  from source** (see the `Optional` note below).
- **On-disk (cross-project):** the **compiled** body = G's `TypedSpec`, keyed by
  function FQN. **The parsed `ValueSpecification` is never serialized cross-project** —
  shipping the parser AST and re-type-checking it in the consumer is the
  recompilation that separate compilation exists to avoid (and drags in the dep's
  transitive signatures, defeating one-hop). Mature systems ship the compiled
  product: rust serializes `Ty` + MIR (never HIR), Java ships bytecode, GHC ships
  Core unfoldings.

So the **`.legend` artifact = `Typed*` signature records (F output) + per-function
`TypedSpec` (G output), all FQN-keyed.** No `Compiled*` record family, no
per-element body tables.

> **Consequence:** `compiler/element/Typed*` records are signatures + FQN refs —
> pure-data, acyclic, serializable from day one. The only thing G compiles, and the
> only thing that ships as a compiled body, is a `Function`'s `TypedSpec`.

**Terminology (pinned).** Three names, three distinct things — there is **no**
`Compiled*` record family:
- **`TypedFunction`** — F's output: a function's *signature* (own `Type` +
  `Multiplicity`, FQN refs) plus an **`Optional<ValueSpecification>` body** — the
  parsed, un-type-checked input to G. One per function; written once at F, never
  gains a compiled-body field. **The body's optionality is origin-determined:**
  *present* for functions compiled from source, *absent* for functions loaded from
  a `.legend` dependency (you hold the dependency's `TypedSpec` instead — you never
  re-derive its body). This is the rust split exactly: the current crate has HIR;
  upstream crates ship only `Ty` + MIR with no HIR. So `.legend` simply does not
  serialize the body field — it ships the signature + the FQN-keyed `TypedSpec`.
  **Note this keeps `Function` as a single record:** a hypothetical split into a
  signature-only type plus a body-bearing type would be a record and its own
  projection (a body is useless without its signature), would break the
  one-element-kind = one-`Typed*`-record symmetry with `TypedClass`/`TypedEnum`,
  and buys no exhaustiveness (the body has exactly one consumer — G). `Optional`
  models the source-vs-dependency *state* directly; subtyping would over-model it.
- **`TypedSpec`** — G's output: the *type-checked, compiled body* of a function,
  produced on demand and memoized in an FQN → `TypedSpec` side table. This is the
  body form that ships in `.legend`.
- **"compiled function"** is **not** a record — it is just a `TypedFunction`
  signature + its `TypedSpec` body, both FQN-keyed. (Earlier drafts called this a
  `CompiledFunction`; there is no such type.)

### 1.1 Is the one-record collapse safe? (homework)

**Yes — validated against the engine's own code and against rustc/Roslyn.** The
engine's `m3` is *already* the shape we want: `PureClass`/`Property`/`PureFunction`
are immutable records with FQN-string refs (no live pointers), no caches, no
cycles, navigation computed via `ModelContext.findClass(fqn)`, and
`PureFunction.body` held as raw `ValueSpecification` ("not type-checked yet").
The engine is even mid-migration *toward* this collapse (`PureFunction`'s unbuilt
`FunctionBody (Resolved | Lazy)` plan for cross-project demand-driven compilation).
**rustc** (pure HIR/metadata + memoized `typeck`/`TyCtxt` query layer) and
**Roslyn** (immutable symbols + lazily-bound bodies) ship exactly this at scale.

The collapse holds as long as these **five disciplines** hold (all already
validated by `m3`):

1. **FQN-string refs only** in records — no live `Typed*` pointers ⇒ no cycles,
   trivially serializable (`PureClass.superClassFqns`, nominal `Property.type`).
2. **No caches / mutable state in records.** Navigation + compiled-body
   memoization live in `ModelContext` / `SpecCompiler` side tables (the rustc
   `TyCtxt` pattern), never in the data records.
3. **Back-references not embedded.** Association-injected nav properties are
   resolved at lookup via `ModelContext`, never stored on `TypedClass`. (Engine
   learned this: `CompiledBackRefFragment`, "never mutate `CompiledClass`" —
   embedding breaks separate/cross-project compilation. `m3.PureClass.properties`
   is already local-declared-only.)
4. **Signatures ship resolved; bodies ship compiled (`TypedSpec`), never the raw
   AST.** A consumer type-checks against a dependency's *resolved signature*
   (always in the artifact) and never needs the dep's body for that — the common
   cross-project case. When a consumer *executes* against a dep body, the artifact
   supplies the **compiled** `TypedSpec` (G's output), so it never re-type-checks
   the dependency. Serializing the raw `ValueSpecification` and recompiling it in
   the consumer is the anti-pattern (rust ships `Ty` + MIR, not HIR; Java ships
   bytecode; GHC ships Core) — it reintroduces recompilation and drags in the dep's
   transitive signatures. The raw body stays **producer-local**.
5. **Performance via a side-index, not live refs.** If FQN lookup ever becomes a
   hotspot, add interning/caching in `ModelContext`; records stay pure. (Engine
   10K spike: ~133 ms to type-check everything — scale is a non-issue.)

**Caveat:** nobody has shipped the *fully* collapsed single-record version in this
codebase yet (the engine still runs both `m3` and `compiled`). The five
disciplines are the guardrails; the direction is validated by the engine roadmap
and by rustc/Roslyn at larger scale.

### 1.2 Body lifecycle: there is exactly one, and it belongs to `Function`

Because F-6 externalizes every Pure body into a `Function`, this is the **only**
body lifecycle in the system — classes, services, constraints, and mappings have
no body lifecycle of their own; they hold an FQN ref into this one. A
`TypedFunction` record is **written once at F and never gains a compiled-body
field.** The compiled body is a derived, on-demand, memoized product of G — not a
mutation of, nor a later field on, the element.

```
Parse (C)   ValueSpecification              raw AST ───────────────┐
                                                                   │ carried verbatim
F           TypedFunction {                                        ▼
              signature: own Type + Multiplicity (FQN refs)
              body: ValueSpecification   ← producer-local input to G, untouched by F
            }                            (in-memory only; the artifact ships TypedSpec)

G           SpecCompiler(body, ModelContext) → TypedSpec
              memoized in a SIDE TABLE (FQN → TypedSpec); element record unchanged
```

- The **raw body** (`ValueSpecification`) is **producer-local** — input to G,
  never serialized cross-project. F type-checks *signatures* only and leaves the
  body untouched (invariant 7). The in-memory signature record is shared with the
  on-disk artifact; the *body* that ships is the compiled `TypedSpec`, not the raw
  AST.
- The compiled `TypedSpec` is stored in a **side table keyed by FQN** — rustc's
  `TypeckResults` (separate from the HIR item) / Roslyn's `BoundNode` (separate from
  the symbol). The element is identical whether or not G has run.
- **No `compiledBody` field, ever.** Rationale: records are immutable; rebuilding a
  variant *with* a compiled slot would make an element's shape depend on whether G
  ran (order-dependent, leaky); and it would couple F's `TypedElement` world to G's
  `TypedSpec` world, breaking "F never triggers G" + discipline #4 (the compiled
  body is a serialized *side* artifact, never an element field).

**Status / parse-vs-externalize:** `SpecParser` already exists (Phase C, complete
through C.5), so at *parse* time `FunctionDefinition.body` and `ClassDefinition`
derived-property / constraint bodies are **already** `List<ValueSpecification>`
(eager parsing does not violate invariant 7 — that forbids eager *type-checking*,
not parsing). **E then externalizes** the class/service/mapping bodies into
synthesized `FunctionDefinition`s (F-6), so by the time F runs, every
`ValueSpecification` body is the body of a `Function`. G turns it into `TypedSpec`
on demand. (`core/README.md`'s `[ ] C.1–C.5` checkbox is stale.)

---

## 2. What may `compiler/element/` import from `parser/`? (the layering rule)

This is the core of the question that prompted this doc. Real compilers (javac
`JCTree`→`Symbol`/`Type`; Roslyn `SyntaxTree`→`SemanticModel`; rustc AST→HIR;
TypeScript AST→`Type`) all have the typed layer **consume** the parse AST and
**produce** their own model. The discipline is about the *output*, not the input.

| Concern | Rule | Why |
|---|---|---|
| **def records** (`ClassDefinition`, …) | `ElementCompiler` **may read** them as input | Standard; the compiler's whole job is def → typed |
| **Typed type** (`Type`) | Owned by `compiler/element/type/Type` | Engine parity (`m3.Type`); no parser import in signatures |
| **Typed multiplicity** (`Multiplicity`) | Owned by `compiler/element/type/Multiplicity`; **convert** `parser.Multiplicity` → typed at the F boundary | **Answers the multiplicity question:** reusing `parser.Multiplicity` leaks `com.legend.parser.*` into every typed consumer. Engine keeps `m3.Multiplicity` separate from its parser for exactly this reason. |
| **Bodies** (all Pure bodies) | Externalized in E into `Function`s (F-6); only `TypedFunction` holds a `ValueSpecification`, copied through F verbatim — **never type-checked in F** | `core/README.md` invariant 7. After F-6 the value-spec AST dependency is confined to `TypedFunction` (not classes/services) → Open Decision **F-1**. |

**Net:** typed *signatures* (`Type` + `Multiplicity`) carry **zero** dependency
on `parser`. Typed *bodies* carry a contained, deliberate dependency on the
value-spec AST, which today lives under `parser.spec` (F-1 decides whether that
package boundary should move).

---

## 3. The typed type system (`compiler/element/type/`)

Mirrors `engine.m3.Type` / `m3.Multiplicity`. Pure-data; FQN strings only.

```
sealed interface Type permits Primitive, ClassType, EnumType, RelationType, FunctionType
  enum   Primitive            // String, Integer, Float, Decimal, Boolean, Number, Byte,
                              // Date, StrictDate, DateTime, LatestDate (from builtin/Pure native classes)
  record ClassType(String fqn)
  record EnumType(String fqn)         // ← NOT in core/README's type/ list; see F-2
  record RelationType(List<Column> columns)   // column = name + Type + Multiplicity
  record FunctionType(List<Param> params, Param result)

sealed interface Multiplicity permits Bounded, Var   // own type; mirrors m3.Multiplicity
```

**Classification boundary (the controlled `NameRef` → `Type` step).** Property
types arrive from the parser as `TypeExpression.NameRef("model::Person")` (FQN
after D). `ElementCompiler` classifies each via `ModelContext.findType(fqn)`,
which resolves **primitives first, then class, then enum** (engine
`ModelContext.findType` parity) — FQN-only by contract. This is the single place
a name becomes a kinded `Type`; everything downstream switches on the sealed
`Type`, never re-parses a name.

**Primitives come from `builtin/Pure`.** In this codebase primitives are bootstrap
native `Class`es (`Pure.STRING`, `Pure.INTEGER`, … under
`meta::pure::metamodel::type::`). `Primitive` is the kinded enum view of that
known FQN set; the bootstrap classes are merged into `ModelContext` so user
classes referencing `String`/`Integer` resolve.

---

## 4. `ModelContext` — the single lookup choke point

**Interface, not a concrete class** (engine parity: `com.gs.legend.model.ModelContext`).

```java
public interface ModelContext {
    Optional<TypedClass>    findClass(String fqn);
    Optional<TypedEnum>     findEnum(String fqn);
    List<TypedFunction>     findFunction(String fqn);   // overloads; the SINGLE body resolver
    Optional<Type>          findType(String fqn);        // primitive→class→enum, FQN-only
    // member resolution: stored + derived + association-injected nav props,
    // walking superClassFqns. Returns a polymorphic Property (Stored | Derived);
    // Derived carries its body fn FQN. Association nav props are resolved HERE via
    // the association index — NOT stored on TypedClass (see §5 discipline 3).
    Optional<Property>      findProperty(String classFqn, String name);
    // structural walks go THROUGH findClass so lazy/cross-project resolves transparently:
    default boolean isSubtype(String childFqn, String parentFqn) { … walk superClassFqns via findClass … }
    // … findTable, association navigation, etc. added per consumer demand …
}
```

**No `findDerivedProperty`, no `findConstraint`.** A derived property is found by
the same `findProperty` member-walk as a stored one (you don't know stored-vs-
derived until you've found it — splitting would force callers to try-each); the
result is a polymorphic `Property` where `Derived` carries the **body function's
FQN**. The body itself — for derived properties, constraints, services, and
functions alike — is resolved by the **one** `findFunction(fqn)`. Constraints
aren't a lookup at all: `TypedClass` holds an ordered list of FINOS-parity
constraint metadata `(name, externalId?, level, predicateFqn, messageFqn?)` (F-6)
and validation calls `findFunction` per entry.

**Association-injected navigation properties are found the same way.** They are
*not* stored on `TypedClass` (§5 discipline 3 — embedding back-references breaks
cross-project compilation); `findProperty` resolves them at lookup time from the
association index. So callers get **one** uniform member API — `findProperty` —
whether a property is stored, derived, or association-derived.

**Signatures are mirrored, bodies are not.** A derived property's signature
(`name/type/multiplicity/params`) lives on `TypedClass` so type-checking
`$person.fullName` answers the type from the class **alone** — no function load —
exactly like a stored property and consistent with the lazy/cross-project
discipline. Only the *body* is fetched (lazily, via `findFunction`) when actually
compiled/executed.

Why an interface matters: every subtype/LCA/property-inheritance walk calls
`findClass(fqn)` rather than dereferencing a live `superClass` pointer
(`m3.PureClass.findProperty`/`allProperties` do exactly this). A future
dependency-backed implementation (`addDepElementDir` + lazy load on cache miss,
`BAZEL_IMPLEMENTATION_PLAN.md` Phase C) drops in behind the same contract with
**no consumer changes**.

The concrete F implementation wraps the existing parsed index
(`compiler/ModelBuilder`) + the bootstrap `builtin/Pure` elements and serves the
compiled `Typed*` map.

---

## 5. Door-keeping: avoiding the cross-project one-way door

The engine's cross-project story (`BAZEL_IMPLEMENTATION_PLAN.md` A/B/C) is a whole
subsystem. Its load-bearing decision — **Phase A: FQN strings, never live refs**
— is the actual one-way door. `core/README.md` already encodes it (invariant 11).
We honor it from line one.

**Bake in now (cheap now, rewrite later):**

1. `TypedClass.superClassFqns: List<String>`; `TypedProperty.type` = `Type` whose
   `ClassType`/`EnumType` hold **FQN strings only** — no resolved payload.
2. `ModelContext` is an interface; `findClass`/`findEnum`/`findFunction`/`findType`
   are the **sole** structural-access path. No walker dereferences live refs.
3. `Typed*` are pure-data, acyclic records → serializable later with no reshape.
4. All bodies are `Function`s (F-6); only `TypedFunction` holds raw
   `ValueSpecification` (invariant 7); F never triggers G.

**Defer (sits behind `findClass`, provably not a one-way door):** `.legend`
serialization, `addDepElementDir` lazy loading, back-reference sidecars,
`compileAll`/`validateElement`. None require touching consumers when added.

---

## 6. Per-kind rollout order

"All elements, but not all at once." Each step is its own tested slice; each adds
one `TypedElement` permit + its `ElementCompiler` arm.

1. **`TypedClass` + `TypedEnum`** — the type-system foundation, inheritance,
   property typing (primitive/class/enum). Establishes `type/` + `ModelContext`.
2. **`TypedAssociation`** — association ends are class properties; nav lookups.
3. **`TypedDatabase`** (+ `store/` `Table`/`Column`/`Join`).
4. **`TypedFunction`** — signature typed; body stays raw. This is the **body
   substrate** every other element's body desugars onto (F-6).
5. **`TypedMapping`** — staging only; `core/README.md` slates it for removal once
   E desugars mappings into `TypedFunction` (see F-5).
6. **`TypedConnection` / `TypedRuntime` / `TypedService` / `TypedProfile`.**

Derived-property and constraint **body externalization** (F-6) is part of E (the
same desugar pass that handles mappings), so by step 1 a `TypedClass` already
carries derived-prop signatures + constraint metadata + body-function FQN refs,
and the synthesized functions land in step 4's substrate.

Until a kind is implemented, its `ElementCompiler` arm throws an explicit
"not yet" (`core/README.md` invariant 5 — no `default ->`).

---

## 7. Open decisions

### F-1. Should the value-spec AST move out of `parser/` so the typed world never imports `parser`? (the generalization of the multiplicity question)

**First, split the concern in two — they are NOT the same, and G only touches one.**

- **Signatures** (property/param/return `Type` + `Multiplicity`). These are *not
  expressions*; `SpecCompiler` (G) never processes them. A `parser`-typed
  signature would therefore be a **permanent, pervasive** dependency on every
  `Typed*` element that nothing downstream ever resolves away. **This is why we
  own `type/{Type,Multiplicity}` and convert at the F boundary (§2, F-4).** Not
  optional, not deferrable.
- **Bodies.** After F-6, every Pure body is a `Function` body, so the **input to
  G** is always a `Function`'s `ValueSpecification`: `SpecCompiler(ValueSpecification,
  ModelContext) → compiler/spec/TypedSpec`. `TypedSpec` is fully typed-world (no
  `parser` dep).

So the *only* residual `parser` reference in the typed world is the **raw body**
held on `TypedFunction` — F-6 confines it to that single element type (classes,
services, etc. hold only signatures + FQN refs, zero `parser` dependency).

- **Persistent, not transient.** Invariant 7 keeps the raw `ValueSpecification` on
  `TypedFunction` for the element's life; G runs **on demand** and yields a
  `TypedSpec` *alongside* (memoized per query, like engine's `CompiledExpression`)
  — it does **not** rewrite the element. So `compiler/element → parser.spec` is a
  standing import, not a passing one.
- **But benign — and never referenced downstream as AST.** The raw `ValueSpec` is
  **G's private input** (like un-type-checked source / HIR), retained only until G
  runs; the consumable, shippable IR is G's `TypedSpec` (the "bytecode"). **No
  consumer downstream of G ever traverses or switches on the raw AST** — they use
  `TypedSpec`, compiled lazily and memoized. Cross-project this is *forced*: a
  dependency ships only its `TypedSpec`, never its `ValueSpec`, so the raw AST
  literally does not exist in a consumer project. The dependency we accept is
  therefore on the AST as a **pure-data record** (not parser *machinery* — lexer,
  `ElementParser`, `SpecParser`), held purely as G's input.
- **Engine precedent.** Engine files the same data in a **neutral**
  `com.gs.legend.ast` package (peer of `parser`), so its typed layer imports
  `ast`, never `parser`. core/ nests it under `parser.spec` (`core/README.md`'s
  `parser/spec` ↔ `compiler/spec` symmetry). Pure package naming.

- **Options (for the body half only).**
  1. **Accept** `compiler/element` → `parser.spec` for raw bodies. Smallest;
     matches README symmetry; correct framing ("AST is data, not the parser").
  2. **Hoist the value-spec AST + `Multiplicity` + `TypeExpression`** into a
     neutral `com.legend.ast` package that both `parser` and `compiler` depend on.
     Removes every `parser` import from the typed world. **Mechanical and
     reversible** (rename + import update, ArchUnit-guarded) — *not* a one-way
     door like FQN-vs-live-ref, so it can be done anytime.
- **Leaning:** **Option 1 now.** The signature half (the real, permanent leak) is
  fixed at F regardless; the body half is by-design and benign. Option 2 is an
  optional later cleanup for naming purity, deferrable because it is reversible.

### F-2. Add `EnumType` to `compiler/element/type/`

`core/README.md`'s `type/` list (`Type, Primitive, ClassType, RelationType,
FunctionType, Multiplicity`) omits `EnumType`, but enums are real elements and
properties can be enum-typed (engine `Property.typeFqn` switches over
`Primitive`/`ClassType`/`EnumType`). **Proposal:** add `EnumType(String fqn)` and
update the README layout. Low risk; required for correct property typing.

### F-3. `ModelContext` is an interface

`core/README.md` lists `compiler/ModelContext.java` as "read-only model view"
without specifying interface vs class. **Proposal:** interface (§4), for the
lazy-load seam. The concrete F impl is a package-private/`final` class.

### F-4. `parser.Multiplicity` → `type.Multiplicity` conversion lives in `ElementCompiler`

A single boundary converter; typed signatures never see `parser.Multiplicity`.
(Direct resolution of the original question.)

### F-5. `TypedMapping` staging

`core/README.md` marks `TypedMapping` "slated for removal post-step-E." E
desugars `MappingDefinition` → synthetic `FunctionDefinition`, so by F most
mapping logic is already a function. **Proposal:** keep `TypedMapping` minimal
(only what E does not desugar — e.g. enumeration mappings / store bindings) and
revisit once E is implemented.

### F-6. Body home: externalize every Pure body to a `Function` (**RESOLVED → option A**)

**Decision:** all Pure `ValueSpec` bodies (function bodies, service queries,
derived properties, constraints, mapping transforms) desugar in **E** into
synthesized `Function`s in the global function registry, addressed by
owner-qualified FQN. Owning elements keep **signatures + FQN refs**, never bodies.
The sole compiled body is a function's `TypedSpec` (G's output, FQN-keyed); the
sole body resolver is `findFunction`. (There is no `CompiledFunction` record — see
the pinned terminology in §1.)

**Inventory that drove this** (verified against the parser records — the only
`ValueSpecification`-bearing def fields in legend-lite):

| Def record | Field | Fate under F-6 |
|---|---|---|
| `FunctionDefinition.body` | `List<ValueSpecification>` | **keeps it** — it *is* the function |
| `DerivedPropertyDefinition.expression` | `List<ValueSpecification>` | externalize → fn |
| `ConstraintDefinition.expression` | `ValueSpecification` | externalize → fn |
| `ServiceDefinition.functionBody` | `ValueSpecification` | externalize → fn |
| M2M mapping `~filter` / property bindings | `ValueSpecification` | already externalized by E (`withMappingFunctions`) |

`PropertyDefinition` has **no** default-value field in legend-lite (FINOS has one;
we don't), so there is no hidden `ValueSpec` site there. (Relational
mappings/databases carry `RelationalOperation`, a *different* sub-language, already
desugared into Pure functions by E — out of scope for this `ValueSpec` axis.) **If
a property default is ever added, route it through the same externalization.**

**Invariant (enforced):** in the **typed model**, `ValueSpecification` is a field
on **exactly one** element — `TypedFunction`. No other `Typed*` even declares one.
Guard with an **ArchUnit rule**: no type in `compiler/element/` except
`TypedFunction` may reference `parser.spec.ValueSpecification`.

This invariant is about the *typed model*, not the parse layer. The two layers
hold the body for different reasons, and **both are legitimate** — this is the same
source-vs-compiled split as `ValueSpec` vs `TypedSpec`, not redundancy:
- **def/parse layer** (`ServiceDefinition.functionBody`, …) retains the body as the
  **syntactic** form — the authoritative source used to render the element back to
  Pure grammar / the FINOS protocol's inline lambda. Producer-local; never ships
  cross-project.
- **typed/semantic layer** holds only an FQN ref into the externalized function —
  the form G type-checks and executes.

E is the boundary that derives the second from the first (once, immutably), so
there is no "two sources of truth" hazard. We deliberately do **not** overwrite the
def-record body with the FQN: doing so would discard the syntactic form and force
an awkward, lossy *un-externalize* step to print/serialize the element — exactly
what FINOS avoids by keeping the inline lambda.

**Bonus — F-1 collapses to one file.** Once only `TypedFunction` references
`ValueSpecification`, the residual `compiler/element → parser.spec` dependency is a
single import in a single class (trivially ArchUnit-guarded), which also makes the
F-1 "hoist the AST to a neutral package" option nearly free if ever wanted.

**Two refinements folded in:**
- A **constraint is not a bare function** — it carries FINOS-parity metadata, kept
  on the class; only its *bodies* externalize. We are **functionally** compatible
  with FINOS (same features), not byte-compatible (different storage):
  ```
  TypedConstraint(
    String           name,          // required
    String           externalId,    // optional — FINOS parity
    EnforcementLevel level,         // Error | Warn (FINOS stores a String; we use an enum, default Error)
    String           predicateFqn,  // synthesized  this -> Boolean[1]  function
    String           messageFqn     // optional synthesized  this -> String[1]  function
  )
  ```
  FINOS `Constraint` is `{name, functionDefinition, externalId, enforcementLevel,
  messageFunction}` evaluated as `predicate(this) -> Boolean`; we keep every
  feature but store the predicate/message as externalized function FQNs and the
  level as an enum.
- **Signatures are mirrored** on the owning element (derived-prop
  `name/type/mult/params`); only the body externalizes (§4).

**Rejected — option B (member-scoped bodies, engine `qualifiedProperties` style):**
keeps the function *shape* but the compiled body then needs a `(class, name)`-keyed
per-member home — the side-car structure this whole design exists to eliminate.
Rejected on the project's own no-side-car criterion, not on collapse-for-its-own-sake.

**FQN convention for synthesized body functions:** `<ownerFqn>$<kind>$<memberName>`.

| Body site | Synthesized FQN |
|---|---|
| derived property `fullName` on `model::Person` | `model::Person$prop$fullName` |
| constraint predicate `validAge` | `model::Person$constraint$validAge` |
| constraint message `validAge` | `model::Person$constraintMsg$validAge` |
| service query for `model::MyService` | `model::MyService$query` |
| mapping class-fn (today `MyMapping_Person`) | `model::MyMapping$mapping$Person` |

`$` is not a legal Pure source-identifier character (it is the variable sigil), so
these FQNs **cannot collide** with user-declared or other synthesized functions —
unlike the current `<fqn>_<simpleName>` scheme, which can (two same-simple-named
classes in one mapping). The scheme is **reversible** (split on `$` →
`[ownerFqn, kind, memberName]`; owner has no `$`, names can't contain `$`) and
**self-documenting** (the `kind` tag both labels and lets enumerations filter
synthesized functions out of "user functions"). Overloads need **no** signature
mangling: `findFunction` returns `List<TypedFunction>` keyed by the member path,
and arg-types disambiguate.

**Cost accepted:** the naming convention above + synthesized functions appearing in
the global namespace/artifact (filterable via the `kind` tag). Cheaper than a
bespoke side table, and it reuses the function machinery F/G/serialization already
require.

**Follow-on work this implies:** E grows from "mapping desugar" into the single
"externalize all bodies to functions" pass; `ModelContext` gains polymorphic
`findProperty` (`Stored | Derived`); `findDerivedProperty`/`findConstraint` never
exist. **Migrate the existing mapping `synthFqn`** (`MappingNormalizer`, currently
`<mappingFqn>_<simpleName>`) to the `$mapping$` form for uniformity and to close its
latent simple-name collision — low-risk (synthesized names are never re-parsed from
source).

---

## 8. References

- `core/README.md` — pipeline, layout, naming, invariants 7 & 11 (authoritative).
- `engine` `com.gs.legend.model.ModelContext` — interface shape, `findType`,
  `isClassSubtype`/`findLowestCommonAncestor` (walk via `findClass`).
- `engine` `com.gs.legend.model.m3.{PureClass,Property,Multiplicity,Type}` —
  FQN-string refs, separate typed multiplicity.
- `engine` `com.gs.legend.compiled.CompiledElement` — pure-data serializable form.
- `docs/PHASE_B_COMPILED_ELEMENTS.md`, `docs/BAZEL_IMPLEMENTATION_PLAN.md` —
  three-layer model + cross-project lazy loading (Phases A/B/C).
