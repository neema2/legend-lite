# Legend Lite — Structure vs. Behavior

> **North star for "is this a new concept, or just a function?" decisions.**
> Companion to `TENETS.md` (which governs *when* work happens). This one governs
> *what kind of thing* each language construct is. Read it before adding any new
> element kind that carries an expression.

## The thesis

A Legend model reduces to **two layers**:

- **Structure** — inert, declarative, **expression-free**. Types and the graph
  between them: `Class`, `Enum`, `Association`, `Profile`, `Store`, `Runtime`,
  plus the *shapes and bindings* of mappings/services (a class's members and
  their signatures, which predicates apply to it, which function a service
  routes to). Pure types and edges. **No `ValueSpecification` ever lives here.**
- **Behavior** — every executable body, expressed as a **`Function`**, compiled
  by **one** pipeline.

Everything that *computes* is a function. Everything else is structure that
**references behavior by FQN**. That is the whole model.

> The slogan "everything that isn't a Class/Enum is a Function" is the right
> *direction* but too strong literally — Associations, Stores, Runtimes, and the
> *shapes* of mappings/services are also structure. The precise, defensible
> invariant is: **no expression body lives outside a function; structural
> elements only reference behavior by FQN.**

## One concept, four hats

Derived properties, constraints, service queries, and mapping transforms are
**not four features**. They are one concept — a function — wearing four
structural *hats*. The hat is structure; the function is behavior.

| Surface concept      | Behavior (function)                      | Structural hat (binding)                |
|----------------------|------------------------------------------|-----------------------------------------|
| Derived property     | `Owner$prop$<name>(this, …): T[m]`        | member-of-class (intrinsic)             |
| Constraint           | `Owner$constraint$<name>(this): Boolean[1]` | predicate-of-class (protocol)         |
| Service query        | `Svc$query(…): Any[*]`                     | behavior-of-service (protocol)          |
| Mapping transform    | synth class/assoc function                | transform-of-mapping (protocol)         |

All four synth functions land in the **one** `findFunction` index and are
indistinguishable from user-written functions. Synth FQNs use the reserved `$`
sigil, so they never collide with user-writable names.

## The model we borrow: Rust

Rust is the reference implementation of this split. Its object model is
"structure and behavior are different things, *bound* by a third construct":

- **`struct` / `enum` = structure.** Data declarations carry **no method
  bodies**. (= our structural elements hold no `ValueSpec`.)
- **`trait` = behavior contract.** Method *signatures* only (plus optional
  defaults). (= our member signature / applicability declaration.)
- **`impl` = binding + bodies.** `impl Foo` (inherent) or `impl Trait for Foo`
  (protocol) is a *separate* construct holding the function bodies.

Two enablers make it ergonomic:

1. **Methods are functions with a receiver; dot-call is sugar.**
   `foo.area()` ≡ `Foo::area(&foo)`; `&self` is the first parameter. This is
   exactly our `Owner$prop$<name>(this, …)` + `$p.fullName` plan — Rust
   validates the whole approach.
2. **No inheritance.** Behavior is *added* via impls, never carried in a data
   hierarchy, which keeps the data layer permanently behavior-free.

A coherence rule (Rust's **orphan rule**) keeps the binding layer unambiguous:
there is never more than one body realizing a given member. We need the analog —
a deterministic rule for "which function realizes this member/constraint."

### Rust ↔ Legend

```
Rust                                   Legend
─────────────────────────────────────  ─────────────────────────────────────────────
struct Person { name: String }         Class Person { name: String[1] }      (structure)
impl Person { fn full_name(&self) {} }  fullName() { $this.name }: String[1]  (sugar)
   └─ lowers to  Person::full_name       └─ lowers to  Person$prop$fullName(this) (function)
person.full_name()                      $p.fullName                           (UFCS sugar)
```

Our synth `Person$prop$fullName` **is** Rust's `Person::full_name`; the
derived-property syntax is Rust's inherent-`impl` sugar.

## The binding abstraction: inherent vs. protocol

Steal Rust's **inherent vs. trait** distinction to model the four hats uniformly:

- **Inherent members → derived properties.** Intrinsic to the class; resolved by
  dot-access on the receiver's static type. Like `impl Person`.
- **Protocol impls → constraints, service queries, mapping transforms.** The
  structural element *opts into* a named behavioral protocol; the synth
  function(s) are its impl. Like `impl Validatable for Person`,
  `impl Query for Svc`, `impl Transform for Mapping`.

A **binding** is therefore: `(kind, ownerFqn, memberName/route, functionFqn,
signature)`. Bindings live in the structure layer; bodies live in the function
space. Resolution of `$p.fullName` / constraint applicability / service routing
is binding lookup — never a body walk.

## Two non-negotiable invariants

1. **Structural elements are expression-free and reference behavior by FQN.**
   Signatures (param/return *types*) live in structure — they are types, not
   expressions, so they belong there. The instant there is an *expression*, it
   is a function, referenced by FQN. (This is the compiled-layer reading of
   `TENETS.md`: structural/reference safety is eager+total; body type-checking is
   demand-driven Work on the referenced function.)
2. **Synth functions carry source provenance.** A function lowered from a
   constraint must remember it *was* "constraint X of `Person`," so diagnostics
   speak in user terms, never `Person$constraint$X`. Rust does this by preserving
   the `impl` source span on the lowered item; we add a `synthesizedFrom`
   pointer. Add it *now* — it is painful to retrofit.

## Surface vs. core

- **Core / compiled model**: fully bifurcated — typed structure + a flat function
  space, bound by lightweight inherent/protocol bindings.
- **Surface language**: keeps ergonomic sugar — derived-property syntax,
  constraint blocks, the legacy mapping DSL — all of which **desugar** in
  Phase E (`ModelNormalizer`) into the two-layer core. New projects may instead
  write **plain functions** for derived properties, constraints, service queries,
  and mappings; the structural element supplies only the declaration/binding.
- **Eventual ergonomic endgame**: **UFCS / dot-call dispatch** (`$x.f(a)` ≡
  `f($x, a)`) makes derived-property-as-function pleasant enough that the special
  syntax becomes pure sugar. UFCS is a *separate, larger* language decision
  (overload resolution, ambiguity vs. stored properties) and is **not required**
  for the core architecture.

## Honest limits (do not oversell)

- **Dispatch & applicability are irreducibly structural.** `$p.fullName` must
  resolve; validation must know which predicates to run; a REST path must route
  to a query. Those bindings cannot be "just a function" — they are the
  structural hat. The model is "structure (incl. behavior-bindings) + functions,"
  not "functions only."
- **Domain shape-rules survive as thin per-site checks.** The heavy part
  (expression type inference) unifies into one pipeline. But per-site rules
  remain as small signature/result assertions on the synth function — "a
  constraint returns `Boolean[1]`," "a mapping function covers the required
  target properties." Single pipeline = one body checker + thin per-site signature
  checks, not zero per-site logic.
- **The legacy DSL is relocated, not deleted.** Existing projects need the
  mapping DSL, so its parser/normalizer stays as a desugarer. The win is unified
  *type-checking/execution*, not a smaller front-end.

## Concrete next step (storage = the embodiment)

> **⚠️ Superseded by `CLEAN_SHEET_INVERSION.md`.** The owner-storage
> mechanism below (`FunctionSynthesizer`, `synthFunctions` fields on
> parser records, the bespoke `ModelBuilder` flatten) put Phase-E output
> inside Phase-B records and left the clean-sheet form unparseable. The
> inversion doc replaces it: lifted functions become ordinary elements in
> a `NormalizedModel`, ownership/invalidation moves to a derived
> `liftedByOwner` index, and the four hats become FQN bindings on the
> structural elements. The thesis of this doc (two layers, one function
> space, bindings not bodies) is unchanged — the storage embodiment is.

Behavior is owned by its structural source (for invalidation + provenance) and
flattened into the one function index by a **single generic mechanism**:

- `interface FunctionSynthesizer { List<FunctionDefinition> synthFunctions(); }`
- `MappingDefinition`, `ClassDefinition`, `ServiceDefinition` implement it; the
  parser leaves the field empty, `ModelNormalizer` (Phase E) populates it.
- **One** flatten step in `ModelBuilder.from`: after structural ingest,
  `if (el instanceof FunctionSynthesizer fs) fs.synthFunctions().forEach(this::appendFunction);`
  Remove the bespoke mapping-only flatten.
- Each synth function carries a `synthesizedFrom` provenance pointer.

**Why owner-storage (not top-level append):** derived data co-located with its
source makes the invariant "synth functions ⊆ what their owner currently
declares" structurally true. Re-normalize a changed owner → fresh
`synthFunctions` replace the old atomically; delete the owner → its functions
vanish with it. No FQN-prefix scanning, no `$`-name parsing to recover ownership.
This serves the element-level incrementality goal in `TENETS.md` /
`pipeline-architecture.md`.

**Shared-owner wrinkle:** a `Class` is owner to *both* derived-property (E.2)
and constraint (E.3) functions, so sub-slices must **accumulate** into one
`synthFunctions` list, not overwrite.

## Status

- **E.1 mappings** — owner-stored on `MappingDefinition.mappingFunctions`, surfaced
  via `FunctionSynthesizer.synthFunctions()`.
- **E.2 derived properties** — owner-stored on `ClassDefinition.synthFunctions`
  (`<owner>$prop$<name>(this, …)`), tagged with `Synthesized(hat="prop")`.
- **E.3 constraints** — owner-stored on `ClassDefinition.synthFunctions`
  (`<owner>$constraint$<name>(this):Boolean[1]`), tagged `hat="constraint"`,
  **accumulating** onto E.2's functions on the shared class owner.
- **E.4 service queries** — owner-stored on `ServiceDefinition.synthFunctions`
  (`<svc>$query(<params>):Any[*]`), tagged `hat="query"`. Typed-lambda query
  params become the function's params; the bare `|body` form is zero-param.
  Return is `Any[*]` (concrete result type is a Phase-G concern).

All synth functions flatten into the one `findFunction` index through a single
generic pass in `ModelBuilder.from` over every `FunctionSynthesizer`.

See also: `TENETS.md`, `MAPPING_LEGACY_TO_FUNCTION.md`,
`MAPPING_UNIFICATION_PLAN.md`, `pipeline-architecture.md`.
