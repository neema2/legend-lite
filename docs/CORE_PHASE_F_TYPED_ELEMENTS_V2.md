# Core Phase F — Typed Elements (`compiler/element/`) — Design (v2)

> **Scope:** `legend-lite/core` Phase F (`ElementCompiler` + `compiler/element/` +
> `ModelContext`). Read `core/README.md` first — this refines its Phase-F blueprint
> (layout ~143–172, invariants 7 & 11). It does **not** touch `engine/`; engine is
> cited only as a reference design.
>
> **Status:** Proposed, pre-implementation. F sits downstream of D and E. **D is done**
> (`core/README.md` Status): D = `parser/ImportResolver` + `ImportScope` (116 tests).
> **E is partially done:** sub-slice **E.1 `MappingNormalizer`** (86 tests) ships; the
> **E.2–E.4** body externalizations (derived properties, constraints, service queries)
> land alongside this work under a new **`ModelNormalizer`** Phase-E entry point. This
> doc pins the shapes so they don't churn when coding starts.
>
> **v2 note:** structural rewrite of v1 (`CORE_PHASE_F_TYPED_ELEMENTS.md`) — same
> decisions, reorganized: the body-externalization decision is promoted from "open
> decisions" to §1, the "ship compiled not AST" argument is stated once (§1.3), and
> a deferred/out-of-scope section (§7) is added. v1 is retained for diffing.

---

## TL;DR

- **F consumes parser output, produces a typed model.** Input: `parser.element.*`
  def records + `parser.spec.ValueSpecification` bodies. Output: `Typed*` — pure-data,
  FQN-referenced, serializable-by-construction. (Every real compiler works this way.)
- **Two collapses define the model (§1).** (1) *Signatures* are one pure-data record
  (no separate in-memory vs on-disk forms). (2) *Bodies* are all `Function`s —
  function bodies, service queries, derived properties, constraints, and mapping
  transforms desugar (in E) to synthesized `Function`s. Owning elements keep
  **signatures + FQN refs**, never bodies.
- **One body, one resolver, no side-cars.** The only compiled body is a function's
  `TypedSpec`; the only body resolver is `findFunction`; there are **no per-element
  compiled-body tables**.
- **Typed signatures own their value types (§2–§3).** `type/` defines its own `Type`
  *and* `Multiplicity`; we never reuse `parser.Multiplicity` in signatures. The only
  residual `parser` reference in the typed world is the parsed (un-type-checked) body
  on `TypedFunction` — an `Optional`, present only for source-compiled functions.
- **`ModelContext` is an interface (§4).** `findClass(fqn)` is the single structural
  choke point, so lazy / cross-project loading drops in later with zero consumer
  changes — the one cross-project one-way door we refuse to slam (§5).

---

## 1. The model: two collapses

Pipeline (`core/README.md`): `… D NameResolver → E ModelNormalizer → F
ElementCompiler → G SpecCompiler …`. **Phase E (`ModelNormalizer`)** is the single
normalization entry point: it externalizes every body site into synthesized
`Function`s, composing **E.1 `MappingNormalizer`** (complex legacy-mapping desugaring,
split out) with **E.2–E.4** body externalization (derived properties, constraints,
service queries, inline). F turns *resolved, normalized def records* into the *typed
model* + a `ModelContext` the rest of the pipeline queries.

The engine (`com.gs.legend`) keeps **three** layers; core/ collapses them to two
distinct concerns (signatures, bodies), each for a concrete reason — not for
collapse's own sake.

| Engine layer | Example | Role |
|---|---|---|
| **def record** | `model.def.ClassDefinition` | parser output; structural source |
| **semantic model** | `model.m3.PureClass`, `Property`, `PureFunction` | what `TypeChecker`/`PlanGenerator` use; exposed via `ModelContext` |
| **compiled artifact** | `compiled.CompiledClass` (`CompiledElement`) | pure-data, no live refs/cycles → serialized `.legend` for cross-project deps |

### Collapse 1 — signatures: one pure-data record

The engine keeps a separate in-memory `PureClass` *and* a serializable
`CompiledClass` for the same structure. core/ keeps **one**, because `m3.PureClass`
is *already* pure-data + FQN-ref — the duplication buys nothing for *signatures*. So
one `TypedClass`/`TypedEnum`/… record serves both the in-memory model and the
on-disk artifact.

### Collapse 2 — bodies: every Pure body is a `Function`

There is exactly one body-bearing element (`Function`) and one compiled-body form (a
function's `TypedSpec`). Phase E already desugars mappings into synthesized
`Function`s; this finishes the move for the other three body sites:

| Pure body site | Desugars (in E) to | Owning element keeps |
|---|---|---|
| function body | (already a `Function`) | — |
| service query | synthesized `Function` | FQN ref |
| derived property `fullName(){…}:T[m]` | `model::Person$prop$fullName(this:Person[1],…):T[m]` | **signature** (name/type/mult/params) + FQN ref |
| constraint `c: <bool over $this>` | `model::Person$constraint$c(this:Person[1]):Boolean[1]` | constraint **metadata** (§1.4) + FQN ref |
| mapping transform / `~filter` | synthesized `Function` (already done in E) | FQN ref |

So **no element other than `Function` carries a `ValueSpecification`.** Classes hold
property/derived-property *signatures* + constraint *metadata* + FQN refs; services
hold config + an FQN ref. Bodies live in the global function registry, addressed by
owner-qualified FQN (§1.5), resolved by the **same** `findFunction` as user functions.

**Why this is the no-side-car design (the point).** Because every compiled body is a
`TypedSpec` keyed by its `functionKey` (§1.5) in the **one** `functionKey → TypedSpec`
table, there is **no per-class / per-member compiled-body side structure** — the thing
that "got out of hand" before. The rejected alternative (member-scoped bodies, engine's
`qualifiedProperties`) keeps the function *shape* but forces the compiled body into a
`(class, name)`-keyed side table — exactly the structure this design eliminates.

> **Consequence:** `Typed*` records are signatures + FQN refs — pure-data, acyclic,
> serializable from day one. The only thing G compiles, and the only thing that ships
> as a compiled body, is a `Function`'s `TypedSpec`. The **`.legend` artifact =
> `Typed*` signature records (F output) + per-function `TypedSpec` (G output), keyed by
> `functionKey` (§1.5).** No `Compiled*` record family, no per-element body tables.

### 1.1 Terminology (pinned)

Three names, three distinct things — there is **no** `Compiled*` record family:

- **`TypedFunction`** — F's output: a function's *signature* (own `Type` +
  `Multiplicity`, FQN refs) plus an **`Optional<ValueSpecification>` body** — the
  parsed, un-type-checked input to G (real AST: parsed by C, name-resolved by D;
  only type-checking is pending — **not** raw text, which D-1 eliminated, and
  **not** "unresolved"). One per function; written once at F, never gains a
  compiled-body field. **The body's optionality is origin-determined:** *present*
  for functions compiled from source, *absent* for functions loaded from a
  `.legend` dependency (you hold the dependency's `TypedSpec` instead — never its
  body). This is the rust split exactly: the current crate has HIR; upstream crates
  ship only `Ty` + MIR, no HIR. So `.legend` simply does not serialize the body
  field — it ships the signature + the `functionKey`-keyed `TypedSpec` (§1.5). **Keeping this as one
  record is deliberate:** a split into a signature-only type plus a body-bearing
  type would be a record and its own projection (a body is useless without its
  signature), would break the one-element-kind = one-`Typed*`-record symmetry with
  `TypedClass`/`TypedEnum`, and buys no exhaustiveness (the body has exactly one
  consumer — G). `Optional` models the source-vs-dependency *state* directly;
  subtyping would over-model it.
- **`TypedSpec`** — G's output: the *type-checked, compiled body* of a function,
  produced on demand and memoized in a `functionKey → TypedSpec` side table (§1.5
  — keyed per-overload, **not** by bare FQN). The body form that ships in `.legend`.
- **"compiled function"** is **not** a record — it is just a `TypedFunction`
  signature (found by FQN lookup) + its `TypedSpec` body (keyed by `functionKey`, §1.5).

### 1.2 Body lifecycle: exactly one, belonging to `Function`

Since every Pure body externalizes into a `Function`, this is the **only** body
lifecycle in the system. A `TypedFunction` is **written once at F and never gains a
compiled-body field**; the compiled body is a derived, on-demand, memoized product of
G — never a mutation of, nor a later field on, the element.

```
Parse (C)   ValueSpecification           parsed AST ───────────────┐
                                                                   │ carried verbatim
F           TypedFunction {                                        ▼
              signature: own Type + Multiplicity (FQN refs)
              body: Optional<ValueSpecification>  ← parsed, un-type-checked; G's input (source-only)
            }                            (in-memory & source-only; artifact ships sig + TypedSpec)

G           SpecCompiler(body, ModelContext) → TypedSpec
              memoized in a SIDE TABLE (FQN → TypedSpec); element record unchanged
```

- The parsed, un-type-checked body is **producer-local** input to G; F type-checks *signatures* only and
  leaves the body untouched (invariant 7).
- The compiled `TypedSpec` lives in a **side table keyed by FQN** — rustc's
  `TypeckResults` (separate from the HIR item) / Roslyn's `BoundNode` (separate from
  the symbol). The element is identical whether or not G has run.
- **No `compiledBody` field, ever.** Records are immutable; a "with compiled slot"
  variant would make an element's shape depend on whether G ran (order-dependent,
  leaky) and couple F's world to G's, breaking "F never triggers G" + §1.3 #4.

**Status / parse-vs-externalize.** `SpecParser` already exists (Phase C), so at
*parse* time function/derived-property/constraint bodies are **already**
`List<ValueSpecification>` (eager *parsing* does not violate invariant 7 — that
forbids eager *type-checking*). **E then externalizes** class/service/mapping bodies
into synthesized `FunctionDefinition`s, so by the time F runs every `ValueSpec` body
is a `Function`'s. (`core/README.md`'s `[ ] C.1–C.5` checkbox is stale.)

### 1.3 Is the collapse safe? And the ship-compiled-not-AST rule (stated once)

**Yes — validated against the engine's own code and against rustc/Roslyn.** `m3` is
*already* the shape we want: `PureClass`/`Property`/`PureFunction` are immutable
records with FQN-string refs (no live pointers), no caches, no cycles, navigation via
`ModelContext.findClass(fqn)`, and `PureFunction.body` held as raw `ValueSpecification`
("not type-checked yet"). The engine is even mid-migration *toward* this collapse
(`PureFunction`'s `FunctionBody (Resolved | Lazy)` plan). **rustc** (HIR/metadata +
memoized `TyCtxt` query layer) and **Roslyn** (immutable symbols + lazily-bound
bodies) ship this at scale.

The collapse holds as long as these **five disciplines** hold (all already in `m3`):

1. **FQN-string refs only** in records — no live `Typed*` pointers ⇒ no cycles,
   trivially serializable (`superClassFqns`, nominal `Property.type`).
2. **No caches / mutable state in records.** Navigation + compiled-body memoization
   live in `ModelContext` / `SpecCompiler` side tables (the `TyCtxt` pattern).
3. **Back-references not embedded.** Association-injected nav properties are resolved
   at lookup via `ModelContext`, never stored on `TypedClass` (engine learned this:
   `CompiledBackRefFragment`, "never mutate `CompiledClass`"; `m3.PureClass.properties`
   is local-declared-only).
4. **Signatures ship resolved; bodies ship compiled (`TypedSpec`), never the raw AST.**
   *This is the canonical statement of the rule referenced throughout the doc.* A
   consumer type-checks against a dependency's *resolved signature* (always in the
   artifact) and never needs the dep body for that — the common cross-project case.
   When it *executes* against a dep body, the artifact supplies the **compiled**
   `TypedSpec`, so it never re-type-checks the dependency. Serializing the raw
   `ValueSpecification` and recompiling it in the consumer is the anti-pattern (rust
   ships `Ty` + MIR, not HIR; Java ships bytecode; GHC ships Core) — it reintroduces
   recompilation and drags in the dep's transitive signatures. The raw body stays
   **producer-local**; downstream of G nothing ever traverses it as AST.
5. **Performance via a side-index, not live refs.** If FQN lookup becomes a hotspot,
   add interning/caching in `ModelContext`; records stay pure. (Engine 10K spike:
   ~133 ms to type-check everything — scale is a non-issue.)

**Caveat:** nobody has shipped the *fully* collapsed single-record version in this
codebase yet (the engine still runs both `m3` and `compiled`). The five disciplines
are the guardrails; the direction is validated by the engine roadmap and rustc/Roslyn.

### 1.4 Constraints: FINOS-functional-parity metadata, our implementation

A constraint is **not** a bare function — it carries metadata kept on the class;
only its *bodies* externalize. We are **functionally** compatible with FINOS (same
features), not byte-compatible (different storage):

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
messageFunction}` evaluated as `predicate(this) -> Boolean`; we keep every feature
but store predicate/message as externalized function FQNs and the level as an enum.
`TypedClass` holds an ordered list of these; validation calls `findFunction` per entry.

### 1.5 Function keys: synthesized FQNs + the compiled-body `functionKey`

`<ownerFqn>$<kind>$<memberName>`:

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
`[ownerFqn, kind, memberName]`) and **self-documenting** (the `kind` tag labels the
function and lets enumerations filter synthesized functions out of "user functions").

**Two keys — one for lookup, one for the compiled body.** These are different keys and
must not be conflated:

- **Lookup key — bare FQN (one-to-many).** `findFunction(fqn)` returns
  `List<TypedFunction>`, the overload *set* under that FQN; the call site
  disambiguates by arity + arg-types. No mangling for lookup — this is exactly what
  `ModelBuilder.findFunction` already does.
- **Compiled-body key — `functionKey` (one-to-one).** The `… → TypedSpec` table (and
  the `.legend` artifact) **cannot** key by bare FQN, because overloaded functions
  share one — a single `FQN → TypedSpec` map would let one overload's body overwrite
  another's. It keys by:

  ```
  functionKey(fqn, params) = params.isEmpty()
      ? fqn
      : fqn + "__" + signatureMangle(params)     // the Pure.java scheme
  ```

  applied **uniformly to every function** — user, native, and synthesized. The no-arg
  case **degenerates to the bare FQN**, so synthesized nullary bodies keep their
  readable `<owner>$<kind>$<member>` form (`model::Person$prop$fullName`); only
  parameterized functions carry the discriminator
  (`model::Person$prop$tax__Float_1__String_0_1`).

**Why uniform, not "mangle only user functions."** Synthesized bodies are **not**
collision-free: a **qualified (parameterized) property** overloads by signature —
`tax(rate: Float[1])` vs `tax(rate: Float[1], country: String[0..1])` both externalize
to `model::P$prop$tax` (the parser already supports parameterized derived properties:
`DerivedPropertyDefinition.parameters`). So an *is-this-overloadable?* branch is both
fragile and **already wrong**. One total key function has no such branch, and the
empty-suffix degeneration preserves readability for the common case — uniformity and
readability at once. The mangle uses **parameter types + multiplicities only**, never
the return type (Pure forbids return-type-only overloads, since the call site can't
disambiguate on them), so a `functionKey` *collision* always means a genuine
redeclaration — a useful failure, not a hazard.

The in-memory memo and the on-disk `.legend` key are the **same** `functionKey`, so
there is exactly one keying scheme. (The engine memoizes the compiled body on resolved
function *identity*, which works in-memory but cannot serialize; `functionKey` is the
stable, content-derived equivalent that crosses the wire.)

**Migrate the existing mapping `synthFqn`** (`MappingNormalizer`, currently
`<mappingFqn>_<simpleName>`) to the `$mapping$` form for uniformity and to close its
latent simple-name collision — low-risk (synthesized names are never re-parsed).
See §7 for the one assumption this rests on.

### 1.6 The one `ValueSpecification` invariant (enforced)

In the **typed model**, `ValueSpecification` is a field on **exactly one** element —
`TypedFunction`. No other `Typed*` even declares one. **Guard with an ArchUnit rule:**
no type in `compiler/element/` except `TypedFunction` may reference
`parser.spec.ValueSpecification`.

This concerns the *typed model*, not the parse layer — and both layers legitimately
hold a body, for different reasons (the same source-vs-compiled split as `ValueSpec`
vs `TypedSpec`, not redundancy):

- **def/parse layer** (`ServiceDefinition.functionBody`, `DerivedPropertyDefinition.
  expression`, …) retains the body as the **syntactic** form — the authoritative
  source for rendering the element back to Pure grammar / the FINOS protocol's inline
  lambda. Producer-local; never ships cross-project.
- **typed/semantic layer** holds only an FQN ref into the externalized function — the
  form G type-checks and executes.

E derives the second from the first (once, immutably), so there is no "two sources of
truth" hazard. We deliberately do **not** overwrite the def-record body with the FQN:
that would discard the syntactic form and force a lossy *un-externalize* step to
print/serialize the element — exactly what FINOS avoids by keeping the inline lambda.

`ValueSpecification`-bearing def fields in legend-lite (verified) and their fate:

| Def record | Field | Fate |
|---|---|---|
| `FunctionDefinition.body` | `List<ValueSpecification>` | **keeps it** — it *is* the function |
| `DerivedPropertyDefinition.expression` | `List<ValueSpecification>` | externalize → fn |
| `ConstraintDefinition.expression` | `ValueSpecification` | externalize → fn |
| `ServiceDefinition.functionBody` | `ValueSpecification` | externalize → fn |
| M2M mapping `~filter` / property bindings | `ValueSpecification` | already externalized by E |

`PropertyDefinition` has **no** default-value field in legend-lite (FINOS has one; we
don't), so there is no hidden `ValueSpec` site (see §7). Relational mappings/databases
carry `RelationalOperation`, a *different* sub-language already desugared by E — out
of scope for this `ValueSpec` axis.

**Bonus:** with only `TypedFunction` referencing `ValueSpecification`, the residual
`compiler/element → parser.spec` dependency (F-1) is a single import in a single
class — trivially ArchUnit-guarded, and the F-1 "hoist AST to a neutral package"
option becomes nearly free.

---

## 2. Layering: what may `compiler/element/` import from `parser/`?

Real compilers (javac `JCTree`→`Symbol`/`Type`; Roslyn `SyntaxTree`→`SemanticModel`;
rustc AST→HIR; TypeScript AST→`Type`) all have the typed layer **consume** the parse
AST and **produce** their own model. The discipline is about the *output*, not the input.

| Concern | Rule | Why |
|---|---|---|
| **def records** (`ClassDefinition`, …) | `ElementCompiler` **may read** as input | the compiler's whole job is def → typed |
| **Typed type** (`Type`) | owned by `type/Type` | engine parity (`m3.Type`); no parser import in signatures |
| **Typed multiplicity** (`Multiplicity`) | owned by `type/Multiplicity`; **convert** `parser.Multiplicity` at the F boundary | reusing `parser.Multiplicity` leaks `com.legend.parser.*` into every typed consumer (engine keeps `m3.Multiplicity` separate for this reason) |
| **Bodies** | externalized in E into `Function`s; only `TypedFunction` holds a `ValueSpecification`, copied through F verbatim, **never type-checked in F** | invariant 7; the value-spec AST dependency is confined to `TypedFunction` → F-1 |

**Net:** typed *signatures* (`Type` + `Multiplicity`) carry **zero** dependency on
`parser`. Typed *bodies* carry a contained, deliberate dependency on the value-spec
AST (under `parser.spec` today; F-1 decides whether that package boundary moves).

---

## 3. The typed type system (`type/`)

Mirrors `engine.m3.Type` / `m3.Multiplicity`. Pure-data; FQN strings only.

```
sealed interface Type permits
    Primitive, PrecisionDecimal,          // scalar leaves
    ClassType, EnumType,                  // nominal (FQN-only)
    TypeVar, GenericType,                 // variables & application (stdlib generics)
    FunctionType, RelationType,           // structural
    SchemaAlgebra                         // relation schema ops

  enum   Primitive            // String, Integer, Float, Decimal, Boolean, Number, Byte,
                              // Date, StrictDate, DateTime, LatestDate — FQNs SOURCED FROM
                              // builtin/Pure (not re-typed); carries family()
                              // (NUMERIC|TEXT|BOOLEAN|TEMPORAL|BINARY) + findByFqn
  record PrecisionDecimal(int precision, int scale)   // DECIMAL(p,s); subtype of Decimal/Number/Any
                              // baked in (NO scattered normalize); DEFAULT_DECIMAL = (38,18)
  record ClassType(String fqn)
  record EnumType(String fqn)                              // ← was F-2 open item; now in
  record TypeVar(String name)                              // T, V, U — generic type parameters
  record GenericType(String rawFqn, List<Type> arguments)  // Relation<T>, List<T>, Function<F>, ColSpec<T>
  record FunctionType(List<Param> params, Param result)    // {T[1] -> Boolean[1]}
  record RelationType(List<Column> columns)                // column = name + Type + Multiplicity
  record SchemaAlgebra(Type left, Op op, Type right)       // T+V, T-Z, Z⊆T, Z=K

sealed interface Multiplicity permits Bounded, Var   // own type; mirrors m3.Multiplicity
  record Bounded(int lower, Integer upper)   // upper == null ⇒ unbounded (*)
  record Var(String name)                    // multiplicity parameter m
```

**Classification boundary (the controlled `NameRef` → `Type` step).** Property/param
types arrive as `TypeExpression` (FQN heads after D). `ElementCompiler` classifies each
via `ModelContext.findType(fqn)`, which resolves **primitive → class → enum** (engine
`findType` parity, FQN-only) for `NameRef` heads, with `TypeExpression.Generic →
GenericType`, type-parameter names → `TypeVar`, and the structural/schema forms mapping
one-to-one. This is the single place a name becomes a kinded `Type`; everything
downstream switches on the sealed `Type`, never re-parses a name.

**Full hierarchy from line one (not the user-element subset).** The generics trio
(`TypeVar` + `GenericType` + `SchemaAlgebra`) and `FunctionType`/`RelationType` are
required to classify the **native stdlib signatures** in `builtin/Pure`
(`Relation<T>`, `Function<{T[1]->Boolean[1]}>`, `Relation<T+V>`, …) — engine resolves
natives to kinded `Type` at parse time, and so do we. Classification *logic* for these
lands incrementally behind `findType`, but the type *records* exist now.

**Primitives come from `builtin/Pure`.** Primitives are bootstrap native `Class`es
(`Pure.STRING`, … under `meta::pure::metamodel::type::`). `Primitive` is the kinded
enum view of that known FQN set — **its FQNs are sourced from the `Pure.*` constants,
not re-typed** — and the bootstrap classes are merged into `ModelContext` so user
classes referencing `String`/`Integer` resolve. The primitive **lattice** (Integer ◁
Number ◁ Any) is the `extends` chain already in `Pure.java`, walked via
`ModelContext.isSubtype` — not re-encoded on `Primitive`.

**`PrecisionDecimal` carries its own subtyping (the engine scar).** Engine's `m3.Type`
records that the legacy precision-decimal *"required 5 scattered normalization copies"*
(normalizing `PrecisionDecimal → DECIMAL` at every comparison site). Core bakes the
DECIMAL/NUMBER/Any equivalence into `PrecisionDecimal` itself and normalizes in the
**one** subtype routine (`ModelContext.isSubtype`) — never at consumer call sites.

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

**No `findDerivedProperty`, no `findConstraint`.** A derived property is found by the
same `findProperty` member-walk as a stored one (you don't know stored-vs-derived
until you've found it — splitting forces callers to try-each); the result is a
polymorphic `Property` where `Derived` carries the **body function's FQN**. The body
itself — for derived properties, constraints, services, and functions alike — is
resolved by the **one** `findFunction(fqn)`. Constraints aren't a lookup at all
(metadata list on `TypedClass`, §1.4).

**Association-injected navigation properties are found the same way** — *not* stored
on `TypedClass` (§5 discipline 3; embedding back-references breaks cross-project
compilation), resolved at lookup time from the association index. So callers get
**one** uniform member API regardless of whether a property is stored, derived, or
association-derived.

**Signatures are mirrored, bodies are not.** A derived property's signature
(`name/type/multiplicity/params`) lives on `TypedClass`, so type-checking
`$person.fullName` answers the type from the class **alone** — no function load —
exactly like a stored property. Only the *body* is fetched (lazily, via
`findFunction`) when actually compiled/executed.

**Why an interface matters:** every subtype/LCA/inheritance walk calls `findClass(fqn)`
rather than dereferencing a live `superClass` pointer (`m3.PureClass.findProperty`/
`allProperties` do exactly this). A future dependency-backed impl (`addDepElementDir`
+ lazy load on cache miss, `BAZEL_IMPLEMENTATION_PLAN.md` Phase C) drops in behind the
same contract with **no consumer changes**. The concrete F impl wraps the existing
parsed index (`compiler/ModelBuilder`) + bootstrap `builtin/Pure` elements and serves
the compiled `Typed*` map.

---

## 5. Cross-project door-keeping

The engine's cross-project story (`BAZEL_IMPLEMENTATION_PLAN.md` A/B/C) is a whole
subsystem. Its load-bearing decision — **Phase A: FQN strings, never live refs** — is
the actual one-way door. `core/README.md` already encodes it (invariant 11); we honor
it from line one.

**Bake in now (cheap now, rewrite later):**

1. `TypedClass.superClassFqns: List<String>`; `TypedProperty.type` = `Type` whose
   `ClassType`/`EnumType` hold **FQN strings only** — no resolved payload.
2. `ModelContext` is an interface; `findClass`/`findEnum`/`findFunction`/`findType`
   are the **sole** structural-access path. No walker dereferences live refs.
3. `Typed*` are pure-data, acyclic records → serializable later with no reshape.
4. All bodies are `Function`s; only `TypedFunction` holds raw `ValueSpecification`
   (invariant 7); F never triggers G.

**Defer (sits behind `findClass`, provably not a one-way door):** `.legend`
serialization, `addDepElementDir` lazy loading, back-reference sidecars,
`compileAll`/`validateElement`. None require touching consumers when added.

---

## 6. Per-kind rollout order

"All elements, but not all at once." Each step is its own tested slice, adding one
`TypedElement` permit + its `ElementCompiler` arm.

1. **`TypedClass` + `TypedEnum`** — type-system foundation, inheritance, property
   typing (primitive/class/enum). Establishes `type/` + `ModelContext`.
2. **`TypedAssociation`** — association ends are class properties; nav lookups.
3. **`TypedDatabase`** (+ `store/` `Table`/`Column`/`Join`).
4. **`TypedFunction`** — signature typed; body stays raw. The **body substrate** every
   other element's body desugars onto.
5. **`TypedMapping`** — staging only; slated for removal once E desugars mappings into
   `TypedFunction` (F-5).
6. **`TypedConnection` / `TypedRuntime` / `TypedService` / `TypedProfile`.**

Derived-property and constraint **body externalization** is part of E (the same
desugar pass as mappings), so by step 1 a `TypedClass` already carries derived-prop
signatures + constraint metadata + body-function FQN refs, and the synthesized
functions land in step 4's substrate. Until a kind is implemented, its
`ElementCompiler` arm throws an explicit "not yet" (`core/README.md` invariant 5 —
no `default ->`).

> **Note (slice dependency):** in steps 1–3, derived-property/constraint FQN refs
> point at synthesized functions whose typed home (`TypedFunction`) arrives in step 4.
> Signature typing is exercisable in step 1; body resolution through those refs is
> not testable end-to-end until step 4.

---

## 7. Deferred / out-of-scope (explicit, so silence isn't mistaken for oversight)

- **Dynamic vs static dispatch of derived properties.** *Static* resolution is handled
  — `findProperty`'s member-walk returns the most-derived declaration, so an override
  resolves to the subclass's `Sub$prop$x` and an inherited one to `Super$prop$x`.
  Whether `$x.prop` dispatches by *declared* type or *runtime* type (when a subclass
  overrides) is an **execution/runtime concern (G and beyond), not pinned here.** Flag
  before G consumes externalized derived-property functions polymorphically.
- **Milestoning / temporal generated properties.** Pure milestoning synthesizes
  dated qualified properties. Assumed to flow through the *same* externalization (E
  generates the derived-property functions), but **not yet specified**.
- **Property default values.** Not present in legend-lite's `PropertyDefinition` today
  (FINOS has them). **If added, route the default expression through the same
  externalization** (`Owner$default$prop`), preserving the §1.6 invariant.
- **`$`-in-FQN serializer assumption (verify before relying on §1.5).** The `$` scheme
  rests on "synthesized FQNs are never re-parsed from source." Confirm nothing that
  *writes/reads* FQNs asserts identifier syntax (`[a-zA-Z_]…`) — specifically the
  `.legend` serializer and the FINOS-protocol JSON writer — or a `$` FQN could break
  there.

---

## 8. Open decisions

**F-6 (body home) is RESOLVED** → externalize every Pure body to a `Function` (option
A). Promoted to §1 (model), §1.4 (constraints), §1.5 (FQN), §1.6 (invariant). Rejected
alternative — **option B (member-scoped bodies, `qualifiedProperties` style)** — keeps
the function shape but forces the compiled body into a `(class, name)`-keyed side
table, the side-car this design exists to eliminate.

The genuinely open items:

### F-1. Should the value-spec AST move out of `parser/`?
The **signature** half is already fixed (own `type/{Type,Multiplicity}`, convert at
the F boundary — not optional). The only residual `parser` reference is the raw body
on `TypedFunction` (§1.6). Options for that body half:
1. **Accept** `compiler/element` → `parser.spec` for raw bodies. Smallest; matches
   README symmetry; correct framing ("AST is data, not the parser").
2. **Hoist** the value-spec AST (+ `Multiplicity` + `TypeExpression`) into a neutral
   `com.legend.ast` package both `parser` and `compiler` depend on. Mechanical,
   reversible, ArchUnit-guarded — *not* a one-way door, doable anytime.
**Leaning: Option 1 now** (the body dependency is by-design, benign, and now confined
to one class per §1.6); Option 2 is an optional later naming-purity cleanup.

### F-2. Add `EnumType` to `type/`
README's `type/` list omits it, but properties can be enum-typed (engine
`Property.typeFqn` switches `Primitive`/`ClassType`/`EnumType`). **Proposal:** add
`EnumType(String fqn)`; update the README layout. Low risk; required for correct typing.

### F-3. `ModelContext` is an interface
README says "read-only model view" without specifying. **Proposal:** interface (§4),
for the lazy-load seam; concrete F impl is a package-private/`final` class.

### F-4. `parser.Multiplicity` → `type.Multiplicity` conversion in `ElementCompiler`
A single boundary converter; typed signatures never see `parser.Multiplicity`.

### F-5. `TypedMapping` staging
E desugars `MappingDefinition` → synthetic `FunctionDefinition`, so by F most mapping
logic is already a function. **Proposal:** keep `TypedMapping` minimal (only what E
does not desugar — enumeration mappings / store bindings) and revisit once E lands.

---

## 9. References

- `core/README.md` — pipeline, layout, naming, invariants 7 & 11 (authoritative).
- `engine` `com.gs.legend.model.ModelContext` — interface shape, `findType`,
  `isClassSubtype`/`findLowestCommonAncestor` (walk via `findClass`).
- `engine` `com.gs.legend.model.m3.{PureClass,Property,Multiplicity,Type}` — FQN-string
  refs, separate typed multiplicity.
- `engine` `com.gs.legend.compiled.CompiledElement` — pure-data serializable form.
- `docs/PHASE_B_COMPILED_ELEMENTS.md`, `docs/BAZEL_IMPLEMENTATION_PLAN.md` — three-layer
  model + cross-project lazy loading (Phases A/B/C).
- `CORE_PHASE_F_TYPED_ELEMENTS.md` — v1 (retained for diffing).
