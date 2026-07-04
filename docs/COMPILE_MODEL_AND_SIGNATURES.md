# Compile — Model + Function Signatures (finish Phase F), then Bodies (Phase G)

**Status:** Plan. Sits on top of `CORE_PHASE_F_TYPED_ELEMENTS_V2.md` (the typed-
element *design* — not re-derived here) and `CLEAN_SHEET_INVERSION.md` (the
canonical representation Phase F now consumes). This doc is the **post-inversion
delta**: what's left to finish Phase F given M1–M7.5, then the outline for
Phase G. It does not restate v2's record shapes.

> **Why a new doc, not an edit of v2.** v2 is the typed-element reference and
> stays authoritative for record shapes. It predates the inversion, so rather
> than mutate it (and risk the competing-doc drift this whole effort removed
> from engine), this doc captures only the delta + the work plan + the
> primitives appendix. v1 (`CORE_PHASE_F_TYPED_ELEMENTS.md`) is superseded by v2.

---

## 0. Where we actually are

Phase F is **mostly built**. `DefaultModelContext` already produces:

- `TypedClass` with stored + derived properties and constraint metadata,
- `TypedEnum`, the typed `Type` system,
- `TypedFunction` **signatures** (`findFunction(fqn)` returns them) — for user
  functions *and* the Phase-E lifted functions, uniformly.

Bodies are **not** type-checked (correct — that's Phase G, demand-driven;
AGENTS.md invariant 7, "F must not trigger G"). So "compile Model + Function
signatures" is the part that's largely done. The gaps below are what the
inversion added that v2 didn't plan for.

---

## 1. The gap: three things the inversion added

### 1.1 Binding integrity (the cheap, high-value first step)

Today nothing checks that a binding's `functionFqn` resolves to a real
function. A clean-sheet mapping referencing a typo'd or missing function sails
through parse → resolve → normalize → F without complaint. This is the
"whose responsibility" item: **reference integrity is a Phase-F concern**,
eager + total, exactly like the existing "Unknown type" check
(`DefaultModelContextTest.unknownPropertyTypeThrows`).

**Work.** When Phase F materializes a structural element that references a
behavior FQN, assert the FQN resolves to a function:

| Reference | Check |
|---|---|
| `Property.Derived.bodyFunctionFqn` | resolves to a function (lifted `$prop$` or a Door-4 bound fn) |
| `TypedConstraint` body FQN | resolves to a function |
| `ClassBinding.functionFqn` (M3) | resolves to a function |
| `AssociationBinding.predicateFunctionFqn` | resolves to a function |
| service `query` FQN | resolves to a function |

Existence only here (reference safety). **Signature shape** ("the mapping fn
returns `Class[*]`", "the constraint returns `Boolean[1]`") is a *thin* check
that can live at F too (signatures are known at F) — but the *body* type-check
is Phase G. Fail loudly with the user-facing name via `synthesizedFrom`
provenance, never the `$`-FQN.

### 1.2 Mapping binding integrity — and why `TypedMapping` is **not** needed

v2 left `TypedMapping` as open decision F-5, written when mappings were the
legacy DSL form (which would have needed desugaring → a typed staging form).
**The inversion resolves F-5: no `TypedMapping`.** The canonical
`MappingDefinition` is already pure-data + FQN-refs (`ClassBinding` /
`AssociationBinding`, no `ValueSpec`, no `Type` to classify — just strings + a
`Kind` enum). By v2's *own* Collapse-1 principle ("a form that is already
pure-data + FQN-ref — the duplication buys nothing for signatures, so we don't
duplicate"), a `TypedMapping` would be a redundant copy. And its only consumer
— dispatch (`Class.all()` under a mapping → binding → FQN → one `findFunction`
index, inversion §6) — is Phase H, unbuilt. Building typed exposure ahead of
its consumer is the premature-surface mistake we deferred M8 for.

So Phase F's mapping work is **validation, not a typed echo**: surface the
canonical binding table as-is when dispatch needs it (Phase H), and *now*
check its reference integrity (§1.1 extended to mappings). When Phase H lands
and a layering concern arises (don't leak `parser.element` types through the
typed `ModelContext`), revisit then — but the record itself isn't compilation
work, it's an adapter, and adapters are cheap to add against a real consumer.

### 1.3 `Realization` is already wired (M7)

Phase F's `realizedFqn(realization, liftedFqn)` already picks the bound FQN for
a Door-4 `Ref` and the lifted FQN for sugar `Inline`, for derived properties and
constraints. `TypedMapping` (1.2) does the same for class/association bindings.
Nothing new to design — just apply the same helper.

---

## 2. Phase G (bodies) — outline only; its own doc when we start it

Phase G is `SpecCompiler`: type-check each function **body** (a
`ValueSpecification`) into a `TypedSpec`, demand-driven, keyed by `functionKey`
(v2 §1.5). It is the **single body type-checker** the whole inversion exists for:
mapping transforms, derived properties, constraints, service queries, and user
functions are *all* functions, so they all type-check through one path. No
per-site body logic.

**The cautionary tale, pinned now so the doc-to-come honors it.** Engine's
`TypeChecker` was a 1,698-LOC god class with a 37-checker dispatch sprawl and a
mapping-resolution *sidecar* smeared across type-checking and lowering. The
plan doc for G must explicitly:

- keep **per-native dispatch** small and uniform (not 37 ad-hoc checkers),
- never reintroduce a resolution **sidecar** (CSI-7 — auxiliary state the body
  is uninterpretable without); resolution is binding-table lookup,
- hold the **F-must-not-trigger-G** line (bodies type-check on demand, never
  eagerly at F),
- memoize by `functionKey`, not by FQN (overloads share an FQN — v2 §1.5).

Phase G is **not** in scope for this doc. Listed so its absence is a choice,
not an oversight.

---

## 3. Work order

1. **F.a — Binding integrity (§1.1). ✅ Landed.** `DefaultModelContext` now
   checks, when it materializes a `TypedClass`, that each derived-property and
   constraint realizing FQN resolves to a real function (`functionExists` =
   native ∪ user ∪ lifted; pure symbol-table lookup, no compilation, no
   recursion). A Door-4 binding to a missing function throws naming the
   user-facing site ("derived property 'fullName' of model::Person binds to
   unknown function 'model::funcs::missing'") — never the `$`-FQN. Sugar passes
   because its lifted `$prop$`/`$constraint$` function is always present. Scoped
   to class members (what F materializes today); mapping/service binding
   integrity lands with F.b. 4 tests, 875 green.
2. **F.b — Mapping binding integrity (§1.2). ✅ Landed.** `validateMappingBindings()`
   runs eagerly at context build (reference safety = Total Knowledge): every
   `ClassBinding` names a real class realized by a real function; every
   `AssociationBinding` names a real association realized by a real predicate.
   A clean-sheet mapping bound to a missing function fails at build in the
   mapping's terms; legacy mappings pass (their lifted `$class$` function is
   present). **No `TypedMapping` record** — the inversion resolved v2's F-5
   (the canonical binding table is already the compiled-ready form; a typed
   echo is a redundant copy whose only consumer, Phase H dispatch, is unbuilt).
   3 tests, 878 green.
3. **F.c — structural signature-shape checks. ✅ Landed.** `requireShape` asserts,
   at the binding site, that the realizing function has the right *structural*
   shape — **no subtyping** (that's G): a constraint returns `Boolean[1]`
   (exact — Boolean is a primitive); an association predicate is
   `(source, target): Boolean[1]`; a mapping class binding is `(): Class[*]`
   (kind check — *which* class is right needs subtyping → G). Sugar/legacy pass
   (correct by construction); a Door-4 binding to a wrongly-shaped function
   fails in the binding's terms. The dividing line: **F = existence + structural
   shape (arity, return-kind, exact-primitive-match); G = type-assignability
   (subtyping, multiplicity subsumption).** 3 tests, 881 green. Deferred to G:
   "returns the *right* class/type" (mappings, derived properties), and service
   queries (expected `Any[*]` is the top type → no shape to violate).
4. **G — bodies.** Its own plan doc + implementation. The payoff.

---

## Appendix A — Clean-sheet primitives (was M8)

The clean-sheet mapping/graph primitives (`MAPPING_CLEAN_SHEET.md` 🚧 markers)
live here, not in the inversion plan: they are **native functions** whose
*signatures* are a Phase-F / builtin-registry concern and whose *semantics*
(type-checking + lowering) are Phase-G+ — i.e. they belong with the consumer
that gives them meaning, not with the parse/representation work that doesn't
depend on them.

| Primitive | Phase-F (signature) | Phase-G+ (semantics) |
|---|---|---|
| **`navigate`** (rename from `associate`; pre-map / inline-slot / post-map forms — `MAPPING_CLEAN_SHEET.md` §3) | native signature(s) in `Pure.java` | multiplicity per position (§3.4); dispatch through the target's mapping; lowering to a join |
| **`map(@Class)`** (1:1 sugar — §5.5) | desugars at parse to `map(r \| ^Class(col=$r.col,…))` when columns match 1:1 | applicability check (every class property has a matching-name/-type column; no class-typed props) is a type-check |
| **`+local`** (mapping-local property — §4.2) | constructor-slot syntax `^Class(+name=val)` | visibility (usable in the mapping pipeline, stripped at consumer boundaries); not part of the class's public API |

**Sequencing.** These are designed **after** Phase G exists, so their semantics
are validated by the type checker rather than guessed ahead of it (the reason
M8 was deferred past F/G — `CLEAN_SHEET_INVERSION.md` decision log). `navigate`
is the most self-contained (an existing `associate` native to build on);
`map(@Class)` and `+local` are new sugar whose payoff is execution.
