# Mapping — Clean-Sheet Function Form

**Status:** Design spec. Informed by `MAPPING_NORMALIZER_DESIGN.md`
(7-layer skeleton), `MAPPING_NORMALIZER_FOLLOWUPS.md` (Waves
roadmap), and `docs/MAPPING_FEATURE_CHECKLIST.md` (full feature
inventory). Treats the function form as the **user-facing language**,
independent of legacy DSL conversion.

A separate doc (`MAPPING_LEGACY_TO_FUNCTION.md`) will cover if/how to
bridge the legacy declarative DSL into this form.

---

## 0. Scope

This doc specifies **what users write** when defining mappings in
function form. It is the API contract. Three orthogonal concerns:

1. **In scope, supported.** Features that the function form is the
   normative way to express. Reachable with current primitives or
   small additions called out below.
2. **In scope, known but deferred.** Features we are aware of
   (catalogued in the feature checklist) but consciously defer.
   Listed so the API surface is forward-compatible.
3. **Out of scope, hard "no".** Features we will not support because
   they break the design's invariants (notably: cross-mapping
   references that bypass another class's mapping, cross-project
   store imports, physical joins to another class's tables).

The split is explicit in §7–§8 at the end.

---

## 1. The big picture — a Mapping is a binding table with per-binding kind tags

```pure
Mapping acme::AcmeMapping
(
    -- Top-level (kind-agnostic) directives
    include other::SharedMapping,
    include other::ProdMapping [db1 -> db2],

    -- Class mappings: per-binding kind tag (`Relational`, `Pure`)
    *acme::Person:                          Relational { acme::funcs::personMapping },
     acme::Person[emp] extends [Person]:    Relational { acme::funcs::employeeMapping },
     acme::Firm:                             Relational { acme::funcs::firmMapping },

     acme::StaffMember:                      Pure       { acme::funcs::staffMapping },

    -- Association mapping: kind tag, body is the predicate function
     acme::Person_Firm:                      AssociationMapping { acme::funcs::personFirmMatch },

    -- Enumeration mapping: kind tag, body is engine's inline static table (data)
     acme::Status:                           EnumerationMapping {
         Active:   ['A', 'ACTIVE'],
         Inactive: ['I']
     }
)
```

The `Mapping` construct is a **binding table**: each binding pairs a
class / association / enum with a realizing body (a function or inline
expression for class / association bindings; a static lookup table for
enum bindings), prefixed by a **kind tag** (`Relational`, `Pure`,
`AssociationMapping`, `EnumerationMapping`).

**Engine compatibility.** This is engine-style syntax — outer
parentheses, per-binding kind tag, same keyword set (`Relational`,
`Pure`, `AssociationMapping`, `EnumerationMapping`). The only
departure is what goes **inside** the kind block's braces: in engine
it's a kind-specific declarative DSL body; in function form it's a
Pure expression returning the appropriate type.

**Why per-binding kind tag (not function profiles, not section headers).**
A function's return type can't disambiguate the kind: both Relational
and Pure (M2M) class mapping functions return `Class[*]`. The kind is
genuinely **a property of the mapping relationship**, not of the
function. Per-binding tags place the label where it semantically
belongs and match engine's existing structure 1:1, easing migration.

Mapping bindings reference **ordinary Pure functions**. The functions
themselves carry no mapping-specific metadata — they're just functions
with the appropriate types. This makes them independently testable,
reusable across mappings, and composable.

### Kind tags and what they bind

| Kind tag | Binding shape | Body type |
|---|---|---|
| `Relational` | `Class: Relational { expr }` (with optional `*`, `[setId]`, `extends [parent]`) | `(): Class[*]` whose body starts from `tableReference(...)` |
| `Pure` | `Class: Pure { expr }` (with optional `*`, `[setId]`, `extends [parent]`) | `(): Class[*]` whose body starts from `OtherClass.all()` |
| `AssociationMapping` | `Association: AssociationMapping { expr }` | `(SourceClass[1], TargetClass[1]) -> Boolean[1]` |
| `EnumerationMapping` | `Enum: EnumerationMapping { Member: [codes], ... }` (engine's inline static table; data, not an expression) | static `Map<Enum, Source[*]>` introspected at parse time |

Kind tag is **required on every binding**, even for single-class mappings.

### Binding rules

- **Class binding** body must be an expression of type `Class[*]`. Can be:
  - A function reference: `Relational { acme::funcs::personMapping }`
  - An inline expression: `Relational { #>{db.T}# -> map(r | ^acme::Person(...)) }`
  - A function call: `Relational { acme::funcs::mappingFor(acme::Person, db1) }`
  - Anything that evaluates to `Class[*]`
- **Association binding** body must be an expression of type
  `(SourceClass[1], TargetClass[1]) -> Boolean[1]`. Same flexibility.
- **Enumeration binding** body is engine's inline static table
  `{ Member: [codes], ... }` — parsed as data, not a Pure expression.
  See Layer 9 for details. Function-form bodies are a deferred future
  revision.
- **Set IDs** are declared inline: `acme::Person[setId]: Relational { f }`.

> **Multiplicity convention in this doc's examples.** Real pure's
> `NewValidator` demands full multiplicity subsumption on `^new(...)`:
> binding a store read (statically `[0..1]` for a nullable column or
> variant key) to a `[1]` property requires an explicit `->toOne()`,
> e.g. `^acme::Person(firstName = $r.FIRST_NAME->toOne())`. The legacy
> desugarer emits this wrapper automatically for `[1]`-declared
> properties; hand-written clean-sheet bodies must spell it. Examples
> below elide `->toOne()` for readability — read every `[1]`-property
> column bind as carrying it.
- **Root marker** is `*` prefix: `*acme::Person: Relational { f }`.
- **Extends** uses `extends [parentId]` after the class spec.

`Class.all()` resolves through the active mapping's binding table
(considers all class mapping bindings — Relational and Pure both).

---

## 2. The mapping function

A class binding's RHS function (or inline expression) has this shape:

```pure
function acme::funcs::personMapping(): acme::Person[*] = {|
  <RELATION-API PIPELINE>                  -- Phase 1: data prep
    -> map(r | ^acme::Person(<fields>))    -- Phase 2: promotion to Class[*]
    -> <CLASS[*]-PIPELINE>?                -- Phase 3: optional post-construction widening
}
```

### Phase 1 — Data prep (any Relation expression)

Phase 1 is **the full Pure Relation API**. Not a curated list of
mapping-specific operations. Anything that returns a `Relation`:

```pure
#>{db.PERSON}#
  -> filter(r | $r.STATUS == 'ACTIVE')
  -> join(~auth: #>{db.AUTH}#, {p,a|$p.ID==$a.PID})
  -> navigate(~firm: acme::Firm.all(), {r,f | $r.FIRM_ID == $f.id})
  -> extend(~[ageBracket: r | if($r.AGE > 65, 'senior', 'adult')])
  -> groupBy(~[firmId: r | $r.firm.id], ~[count: x | $x->count()])
  -> distinct()
  -> someUserDefinedRelationFunction()      -- ordinary Pure function returning Relation
```

`filter`, `join`, `groupBy`, `distinct`, `extend`, `project`, `sort`,
`slice`, `union`, etc. are **not** mapping primitives — they're
Relation API. They have no special status in the function form. The
DSL invented `~filter` / `~groupBy` / `~distinct` as named directives
because the DSL had no pipeline; the function form has the full Pure
pipeline so it inherits everything for free.

The one mapping-specific primitive usable in Phase 1 is `navigate`
(see §3), which widens a Relation with a class-typed sub-row.

### Phase 2 — Promotion (Relation → Class[*])

The universal terminal: `map(r | ^Class(<fields>))`. Constructs Class
instances from rows. When columns match class properties 1:1, sugar
is available: `map(@Class)`.

### Phase 3 — Post-construction (optional Class[*] pipeline)

After `map`, the value is `Class[*]`. Further operations operate on
typed instances:

```pure
... -> map(...) -> filter(p | $p.firm.country == 'US')
                -> navigate(~team: acme::Team.all(), {p,t | $p.teamId == $t.id})
```

`filter` works here too — it's polymorphic on Relation vs Class[*].
`navigate` has an instance-space variant (see §3.3).

---

## 3. `navigate` — graph traversal primitive

`navigate` is the operation for traversing the object graph along
class-typed relationships. It works whether the relationship is
formally declared as an Association or is just an ad-hoc class-typed
property.

Three syntactic positions, all the same conceptual primitive:

> **🚧 Implementation status.** Today only the **post-map (Class[*] widen)**
> form exists as a Pure native (currently named `associate`; rename to
> `navigate` is part of this design). The **pre-map (Relation widen)**
> and **inline (constructor slot)** forms are proposed new primitives
> not yet implemented.

### 3.1 Pre-map (Relation widen) — named sub-row

```pure
#>{db.PERSON}#
  -> navigate(~firm: acme::Firm.all(), {r, f | $r.FIRM_ID == $f.id})
  -> map(r | ^acme::Person(
       firstName    = $r.FIRST_NAME,
       firmName     = $r.firm.name,        -- class property on the navigated sub-row
       firmCountry  = $r.firm.country,
       firm         = $r.firm              -- the whole instance, slot-typed
     ))
```

- Widens each Relation row with a named class-typed sub-row.
- Direct parallel to `join` — but the right side is a class extent
  (dispatches through target's mapping), not a physical table.
- Predicate scope: row-space (`$r` from outer, `$f` is candidate target).
- Multi-hop: each `navigate` step sees prior aliases.
- One declaration, many uses (scalar pulls + full instance both work).

### 3.2 Inline (constructor slot) — sugar for pre-map

```pure
^acme::Person(
  firstName = $r.FIRST_NAME,
  firm      = navigate(acme::Firm.all(), {f | $r.FIRM_ID == $f.id})
)
```

- Lives inside `^Class(...)`, alongside embedded `^Inner(...)` and
  `otherwise(...)` slots.
- Predicate scope: row-space (captures `$r` from outer `map`).
- Multiplicity inferred from slot's declared type:
  `Firm[1]` → first-match required; `Firm[0..1]` → optional first;
  `Firm[*]` → all matches.
- **Semantically equivalent to** `navigate(~__anon: T.all(), {r,t|...})`
  pre-map plus reference to the anonymous alias. Compiler may inline
  or hoist freely.

### 3.3 Post-map (Class[*] widen) — instance-space predicate

```pure
... -> map(r | ^acme::Person(firstName = $r.FIRST_NAME, +firmFk = $r.FIRM_ID))
   -> navigate(~firm: acme::Firm.all(), {p, f | $p.firmFk == $f.id})
```

- Widens `Class[*]` with a class-typed slot via an instance-space
  predicate. Sees `$p` (source instance) and `$f` (target candidate).
- The primary form for **mirrored bidirectional associations** — both
  sides of an Association call the same `(Source, Target) -> Boolean`
  predicate function.
- Needed when the FK is exposed as a class-level property (public or
  `+local`), not just a raw column.
- **Operand asymmetry.** The predicate compares an **owned source term**
  against a **foreign target term**:
  `$source.<ownedTerm> == $target.<classProperty>`. The left may be a
  source class property or a `+local` on the source — we own the source
  mapping, so we can expose whatever key we need. The right must be a
  **target class property** (public or a `+local` the target's mapping
  exposes), never a raw physical column reached from outside the target's
  mapping. This asymmetry is what lets multi-hop work without a quantifier
  (see Layer 5).

### 3.4 Multiplicity semantics across positions

The three positions have **different multiplicity behavior** because
they operate on different containers. This is intentional and mirrors
how `join` already works for to-many physical joins.

| Position | Multi-match behavior | Multiplicity of result |
|---|---|---|
| **Pre-map** `navigate(~alias: T.all(), pred)` | Row count multiplies: each source row × N matching targets = N output rows. Same semantics as `join`. Each output row has one sub-row in `$r.alias`. | Determined by # of matches per source row |
| **Inline** `slot = navigate(T.all(), pred)` | No row multiplication. Slot collects matches per slot's declared multiplicity. | `T[1]` → first/required; `T[0..1]` → first/optional; `T[*]` → collection of all matches |
| **Post-map** `-> navigate(~slot: T.all(), pred)` | No instance multiplication. Slot collects matches per slot's declared multiplicity. | Same rules as inline |

**Guidance:**

- **Many-to-one** (each source matches one target, e.g. `Person.firm`):
  any of the three forms works. Pre-map is most efficient when you need
  multiple scalar pulls from the same target.
- **One-to-many** (each source matches several targets, e.g.
  `Firm.employees`): use **inline** or **post-map** to get a `Class[*]`
  slot. Pre-map would explode row count and require a `groupBy` to
  recollect — almost never what you want.
- **Pre-map for to-many** is legitimate when you genuinely want one
  output row per (source, match) pair — e.g., flattened reporting.

### 3.5 `navigate` and Associations

`navigate` is **the** graph operation. The presence of an `Association`
declaration affects only:
- Multiplicity inference (1 / 0..1 / *).
- Whether the inverse direction is queryable.
- Whether a shared predicate function (in `AssociationMapping`
  binding) is available for both sides.

`navigate` itself doesn't care whether there's an Association declared.

---

## 4. The mapping-specific primitive surface

This is the **entire** vocabulary added to Pure by mapping. Everything
else is Relation API, Pure expressions, or class operations.

| Primitive | Role |
|---|---|
| `Mapping X ( <bindings>... )` | Container: per-binding kind-tagged bindings (`Class: Relational { ... }`, `Class: Pure { ... }`, `Assoc: AssociationMapping { ... }`, `Enum: EnumerationMapping { ... }`) + top-level directives (include, substitution) |
| `navigate(...)` | Graph traversal (3 positions: pre-map, inline, post-map) |
| `^Class(...)` / `^Class($src)` | Construction / positional cast through Class's mapping |
| `otherwise(partial, fallback)` | Generic structural merge of two instances |
| `+local` | Mapping-local class property declaration |

Five things. The function form is just ordinary Pure functions, bound
into a Mapping construct.

Note: enumeration mapping bodies are **engine's inline static table**
(data, parsed at definition time), not Pure expressions. Invocation in
a class mapping uses `^Enum(srcExpr)` — the same positional-cast form
as `^Class($row)`. See Layer 9. Function-form bodies (with `match`,
`^Map(...)`, `Map<K,V>` ops) are deferred.

### 4.1 Constructor forms

| Form | Use |
|---|---|
| `^Class(field=val, ...)` | Named-args construction |
| `^Class(+local=val, ...)` | Named-args with mapping-local properties |
| `^Class($srcExpr)` | Positional cast — feed `$srcExpr` through Class's active mapping |
| `^Outer(<eager>) -> otherwise(<fallback>)` | Hybrid: partial instance + fallback for missing fields |

### 4.2 `+local` — mapping-local properties

> **🚧 Proposed primitive — not yet implemented.** Engine has `+prop: Type[m]: expr`
> in PM declarations; the function form below extends `+` into the
> `^Class(...)` constructor as a named-arg with `+` prefix.

```pure
^Person(
  firstName = $r.FIRST_NAME,
  +firmFk   = $r.FIRM_ID           -- local: usable in pipeline, hidden from consumers
)
```

- Declared via `+name`; accessed via plain `.name` (`$p.firmFk`).
- Visible to subsequent pipeline ops in the same mapping body
  (typically: post-map `navigate` predicates).
- Not part of Class's public API. Stripped at consumer boundaries.
  Other mappings cannot reach a class's locals via `Class.all()`.
- Cannot be self-referenced within the same constructor.

### 4.3 `otherwise` — generic structural merge

> **🚧 Proposed primitive — not yet implemented.** A prior `otherwise(eager, fallback)`
> native existed but was deleted (see `Pure.java:710` comment). This
> spec proposes reintroducing it as a **generic class-level merge**,
> not bolted to embedded mappings.

`otherwise(partial, fallback)` takes a partial instance and a fallback
instance, returns a complete instance with `partial`'s fields where set,
`fallback`'s fields otherwise. It is a **class-level merge**, not a
navigation primitive.

```pure
-- Common case: navigate as fallback
address = ^Address(street=$r.STREET, city=$r.CITY)
            -> otherwise(navigate(Address.all(), {a | $r.ADDRESS_ID == $a.id}))

-- Sugar (equivalent to the above)
address = ^Address(street=$r.STREET, city=$r.CITY)
            -> otherwise(Address.all(), {a | $r.ADDRESS_ID == $a.id})

-- Static fallback
address = ^Address(street=$r.STREET) -> otherwise(^Address(street='UNKNOWN', city='UNKNOWN'))

-- Computed fallback
address = ^Address(street=$r.STREET) -> otherwise(getDefaultAddress($r.regionCode))
```

`otherwise` decouples cleanly from navigation — it's a reusable
class-merge utility.

### 4.4 The cardinal rule

> **A mapping function for class A does not couple A's object-level
> meaning to physical joins of other classes' owned tables.**

Specifically, the body of a function bound as `acme::A: f` MAY reference:

- A's own modeled properties and locals
- Tables in A's own source store, via `tableReference` + `join`
- Other classes' **public API**, via `Class.all()` inside `navigate`
- Target-side property access in `navigate` predicates

It MAY NOT reference:

- Another class's local properties
- Another class's specific mapping function by name (only
  active-mapping dispatch via `Class.all()`)
- Another class's owned physical tables outside this mapping's own
  source-side `join`s

Enforced structurally by primitive split: `join` widens **Relation**
rows (same-store physical); `navigate` widens **class-typed** sub-rows
(cross-mapping). The two are different primitives; not interchangeable.

---

## 5. Composition patterns

The Mapping binding takes any `Class[*]`-typed expression. This
enables factoring the mapping function into separately testable,
reusable parts.

### 5.1 All-in-one external function

```pure
function acme::funcs::personMapping(): acme::Person[*] = {|
  #>{db.PERSON}#
    -> filter(r | $r.STATUS == 'ACTIVE')
    -> map(r | ^acme::Person(firstName = $r.FIRST_NAME))
}

Mapping acme::M
(
  acme::Person: Relational { acme::funcs::personMapping }
)
```

### 5.2 Factored — Relation prep + construction separate

```pure
-- Pure Relation prep. Returns a typed Relation. Testable as SQL.
-- Reusable across multiple class constructions and mappings.
function acme::prep::activePersonRows(): Relation<(PERSON_ID:Integer, FIRST_NAME:String, ...)> = {|
  #>{db.PERSON}#
    -> filter(r | $r.STATUS == 'ACTIVE')
    -> extend(~[displayName: r | $r.FIRST_NAME + ' ' + $r.LAST_NAME])
}

-- Class construction composes prep with ^Class.
function acme::funcs::personMapping(): acme::Person[*] = {|
  acme::prep::activePersonRows()
    -> map(r | ^acme::Person(
         firstName   = $r.FIRST_NAME,
         displayName = $r.displayName
       ))
}

Mapping acme::M
(
  acme::Person: Relational { acme::funcs::personMapping }
)
```

**Why this matters:**

- `activePersonRows()` executable as SQL standalone — assert column
  shapes, row counts, filter behavior without class construction noise.
- Reusable: `Employee` and `Person` mappings can share the same prep.
- Mockable: swap `tableReference` for a Relation literal in tests.
- Library prep functions live in shared packages.

### 5.3 Inline binding

```pure
Mapping acme::M
(
  acme::Person: Relational {
    #>{db.PERSON}# -> map(r | ^acme::Person(firstName = $r.FIRST_NAME))
  }
)
```

### 5.4 Inline binding reusing external prep

```pure
function acme::prep::activePersonRows(): Relation<...> = {|
  #>{db.PERSON}# -> filter(r | $r.STATUS == 'ACTIVE')
}

Mapping acme::M
(
  acme::Person: Relational {
    acme::prep::activePersonRows() -> map(r | ^acme::Person(firstName = $r.FIRST_NAME))
  }
)
```

### 5.5 The chef's-kiss form — prep matches class 1:1

When the data prep produces columns matching class properties 1:1
(including types), the binding collapses to one line:

```pure
function acme::prep::people(): Relation<(firstName:String, lastName:String, age:Integer)> = {|
  #>{db.PERSON}#
    -> filter(r | $r.STATUS == 'ACTIVE')
    -> project(~[
         firstName: r | $r.FIRST_NAME,
         lastName:  r | $r.LAST_NAME,
         age:       r | $r.AGE
       ])
}

Mapping acme::M
(
  acme::Person: Relational { acme::prep::people() -> map(@acme::Person) }
)
```

> **🚧 `map(@Class)` sugar — proposed.** Not implemented in current Pure
> for relational mapping bodies. Specification below.

`map(@Person)` is sugar for `map(r | ^Person(firstName=$r.firstName,
lastName=$r.lastName, age=$r.age))`. Applies when:

1. Input is a Relation row.
2. Every class property has a column of matching name AND compatible type.
3. The class has no class-typed properties (no embedded, no navigate
   slots — those require explicit construction).

When applicable, the mapping is essentially "this class IS this prep's
output, projected through the constructor."

### 5.6 Multi-stage prep composition

```pure
function acme::prep::personBase(): Relation<...> = {|
  #>{db.PERSON}# -> filter(r | $r.STATUS == 'ACTIVE')
}

function acme::prep::personWithFirm(): Relation<...> = {|
  acme::prep::personBase()
    -> navigate(~firm: acme::Firm.all(), {r,f | $r.FIRM_ID == $f.id})
}

function acme::prep::personSenior(): Relation<...> = {|
  acme::prep::personWithFirm() -> filter(r | $r.AGE > 65)
}

Mapping acme::M
(
   acme::Person:       Relational { acme::prep::personWithFirm() -> map(r | ^acme::Person(...)) },
  *acme::Person[snr]:  Relational { acme::prep::personSenior()   -> map(r | ^acme::Person(...)) }
)
```

Set-ID variants share prep. Layered. Pure functions all the way down.

---

## 6. Layered examples — canonical form per scenario

Each layer is a complete, copy-pasteable user-written setup.

### Layer 1 — Primitive properties (single table)

```pure
function acme::funcs::personMapping(): acme::Person[*] = {|
  #>{db.PERSON}#
    -> map(r | ^acme::Person(
         firstName = $r.FIRST_NAME,
         lastName  = $r.LAST_NAME,
         age       = $r.AGE
       ))
}

Mapping acme::M
(
  *acme::Person: Relational { acme::funcs::personMapping }
)
```

With 1:1 sugar (when columns match):

```pure
Mapping acme::M
(
  *acme::Person: Relational { #>{db.PERSON}# -> map(@acme::Person) }
)
```

**Covers:** A1 (simple column), A3 (dyna — write as ordinary Pure expr
in the lambda), A4 (dyna + join — combines with Layer 2).

### Layer 2 — Scalar via physical join (same-store, owned tables)

```pure
function acme::funcs::personMapping(): acme::Person[*] = {|
  #>{db.PERSON}#
    -> join(~firm: #>{db.FIRM}#,
            {p, f | $p.FIRM_ID == $f.ID})
    -> map(r | ^acme::Person(
         firstName = $r.FIRST_NAME,
         firmName  = $r.firm.NAME       -- physical column on joined sub-row
       ))
}
```

- `join` widens with a physical sub-row alias.
- Predicate references physical columns on both sides.
- For when you **own both tables** and don't need target's mapping.

**Multi-hop:**
```pure
... -> join(~firm:    #>{db.FIRM}#,    {p,f|$p.FIRM_ID==$f.ID})
   -> join(~country:  #>{db.COUNTRY}#, {p,c|$p.firm.COUNTRY_ID==$c.ID})
   -> map(r | ^Person(firmName = $r.firm.NAME, countryCode = $r.country.CODE))
```

**Covers:** A2 (join chain + column) when tables co-owned.

### Layer 2' — Scalar via cross-mapping navigate

When target lives in a different store / has its own mapping / you
don't own its tables:

```pure
function acme::funcs::personMapping(): acme::Person[*] = {|
  #>{db.PERSON}#
    -> navigate(~firm: acme::Firm.all(), {r, f | $r.FIRM_ID == $f.id})
    -> map(r | ^acme::Person(
         firstName    = $r.FIRST_NAME,
         firmName     = $r.firm.name,        -- class property pulled through Firm's mapping
         firmCountry  = $r.firm.country
       ))
}
```

- `navigate` widens with a class-typed sub-row.
- Predicate in row-space; references property of target instance.
- Dispatches through Firm's mapping (honors its filters, transforms, etc.).

### Layer 3 — Class-typed property via navigate

The slot is a Class instance, materialized through target's mapping.
Two equivalent forms; choose by use case.

**Inline form (concise; primary):**
```pure
function acme::funcs::personMapping(): acme::Person[*] = {|
  #>{db.PERSON}#
    -> map(r | ^acme::Person(
         firstName = $r.FIRST_NAME,
         firm      = navigate(acme::Firm.all(), {f | $r.FIRM_ID == $f.id})
       ))
}
```

No `+local` needed. No post-map step. The slot is filled inline.

**Pre-map form (equivalent; use when target is referenced multiple times):**
```pure
function acme::funcs::personMapping(): acme::Person[*] = {|
  #>{db.PERSON}#
    -> navigate(~firm: acme::Firm.all(), {r, f | $r.FIRM_ID == $f.id})
    -> map(r | ^acme::Person(
         firstName = $r.FIRST_NAME,
         firm      = $r.firm,
         firmName  = $r.firm.name,        -- bonus: scalar pulls from same navigation
         isUSFirm  = $r.firm.country == 'US'
       ))
}
```

The compiler treats inline as sugar for pre-map with an anonymous
alias. Use inline for single-shot slot fills; use pre-map when the
navigation result is referenced multiple times in the constructor.

**Composite-key:**
```pure
firm = navigate(acme::Firm.all(),
                {f | $r.FIRM_ID == $f.id && $r.FIRM_COUNTRY == $f.country})
```

**Self-association:**
```pure
manager = navigate(acme::Employee.all(), {e | $r.MANAGER_ID == $e.id})
```

### Layer 3' — Class-typed property via physical join + inline ^Class

> **⚠️ Ownership constraint.** Layer 3' is only valid when:
> - The target table (`FIRM` here) lives in the **same store** as this
>   mapping's source (`db`).
> - The target class (`Firm`) is **either**:
>   - Not separately mapped (no `Firm: ...` binding in any reachable Mapping), OR
>   - You are explicitly inline-modeling Firm's data as part of this view.
>
> If `Firm` is mapped elsewhere (in this project or another), use
> **Layer 3** (`navigate`) instead. Otherwise you bypass `Firm`'s
> mapping (its filters, transforms, cross-store joins) and leak
> abstraction — the cardinal sin (§4.4).

When the constraint holds, the inline-construction form is fine:

```pure
... -> join(~firmRow: #>{db.FIRM}#,
            {p, f | $p.FIRM_ID == $f.ID})
   -> map(r | ^acme::Person(
        firstName = $r.FIRST_NAME,
        firm      = ^acme::Firm(id = $r.firmRow.ID, name = $r.firmRow.NAME)
      ))
```

Use Layer 3 (`navigate`) by default. Layer 3' is for inline modeling
without a separate Firm mapping — the target class is essentially
private-to-this-mapping data.

### Layer 4a — Embedded (sub-object on same table)

```pure
... -> map(r | ^acme::Person(
        firstName = $r.FIRST_NAME,
        address   = ^acme::Address(
                      street  = $r.STREET,
                      city    = $r.CITY,
                      country = $r.COUNTRY
                    )
      ))
```

Nested constructor. No intermediate scratch columns.

**Covers:** A6 (embedded).

### Layer 4b — Embedded + Otherwise (hybrid)

```pure
... -> map(r | ^acme::Person(
        firstName = $r.FIRST_NAME,
        address   = ^acme::Address(street = $r.STREET, city = $r.CITY)
                      -> otherwise(navigate(acme::Address.all(),
                                            {a | $r.ADDRESS_ID == $a.id}))
      ))
```

Or sugared:
```pure
address = ^acme::Address(street = $r.STREET, city = $r.CITY)
            -> otherwise(acme::Address.all(), {a | $r.ADDRESS_ID == $a.id})
```

**Covers:** A7 (otherwise embedded).

### Layer 4c — Inline embedded (delegate to another mapping)

When one row maps to multiple class shapes, factor each shape's
construction into its own mapping function and compose via cast.

```pure
-- PersonDetails has its own mapping; defines which columns it reads.
function acme::funcs::personDetailsMapping(): acme::PersonDetails[*] = {|
  #>{db.PERSON}#
    -> map(r | ^acme::PersonDetails(
         email = $r.EMAIL,
         phone = $r.PHONE,
         dept  = $r.DEPT_CODE
       ))
}

-- Person's mapping delegates the details-shape to PersonDetails.
function acme::funcs::personMapping(): acme::Person[*] = {|
  #>{db.PERSON}#
    -> map(r | ^acme::Person(
         firstName = $r.FIRST_NAME,
         details   = ^acme::PersonDetails($r)       -- pass the whole row; cast reuses PersonDetails's column picks
       ))
}
```

**Cast semantics:** `^acme::PersonDetails($r)` means "apply
PersonDetails's mapping logic to this row." PersonDetails's mapping
body contains a construction lambda `{ r | ^PersonDetails(email=$r.EMAIL, ...) }`;
the cast re-applies that lambda with the passed `$r` substituted for
PersonDetails's normal source row. Person's mapping doesn't need to
know which columns PersonDetails uses — that's PersonDetails's
business.

**Constraint:** the passed row must have all columns PersonDetails's
mapping reads. If PersonDetails reads `EMAIL` but the passed row lacks
`EMAIL`, that's a type error. So passing whole `$r` works when both
mappings draw from the same (or a superset) source.

This is the **inline-embedded reuse pattern** — a composition
primitive that lets multiple class shapes share a row source without
duplicating column-pick logic.

**Covers:** A8 (inline embedded).

### Layer 5 — Bidirectional Association (mirrored)

Class-level declaration (in the logical model):
```
Association acme::Person_Firm (
  Person.firm    [1]: Firm,
  Firm.employees [*]: Person
)
```

Shared predicate (an ordinary Pure function):

```pure
function acme::funcs::personFirmMatch(p: acme::Person[1], f: acme::Firm[1]): Boolean[1] = {|
  $p.firmFk == $f.id
}
```

Person and Firm mapping functions:

```pure
function acme::funcs::personMapping(): acme::Person[*] = {|
  #>{db.PERSON}#
    -> map(r | ^acme::Person(
         firstName = $r.FIRST_NAME,
         +firmFk   = $r.FIRM_ID
       ))
    -> navigate(~firm: acme::Firm.all(),
                {p, f | acme::funcs::personFirmMatch($p, $f)})
}

function acme::funcs::firmMapping(): acme::Firm[*] = {|
  #>{db.FIRM}#
    -> map(r | ^acme::Firm(id = $r.ID, name = $r.NAME))
    -> navigate(~employees: acme::Person.all(),
                {f, p | acme::funcs::personFirmMatch($p, $f)})
}
```

Mapping binding:

```pure
Mapping acme::M
(
  *acme::Person:        Relational         { acme::funcs::personMapping },
  *acme::Firm:          Relational         { acme::funcs::firmMapping },
   acme::Person_Firm:   AssociationMapping { acme::funcs::personFirmMatch }     -- shared predicate
)
```

Both sides call the same predicate; param order swaps on the inverse.

#### Multi-hop associations

When the physical path between two ends crosses an intermediate table,
there are two clean-sheet shapes, chosen by whether that intermediate is
modeled. **No multi-hop predicate ever binds an intermediate row — the
intermediate is always handled on the owned source side.**

**Preferred — model the intermediate.** Give the middle table its own
class and split into two single-hop associations. Multi-hop traversal is
then ordinary navigation (`$person.address.city`); there is *no* multi-hop
association artifact. Each class maps only its own table; the FK is a
`+local`; each predicate is `$source.<fkLocal> == $target.id`.

```pure
Association acme::Person_Address ( Person.address [1]: Address, Address.persons [*]: Person )
Association acme::Address_City   ( Address.city   [1]: City,    City.addresses  [*]: Address )

function acme::funcs::personMapping(): acme::Person[*] = {|
  #>{db.PERSON}#
    -> map(r | ^acme::Person(id = $r.ID, name = $r.NAME, +addrFk = $r.ADDR_ID))
}
function acme::funcs::addressMapping(): acme::Address[*] = {|
  #>{db.ADDR}#
    -> map(r | ^acme::Address(id = $r.ID, +cityFk = $r.CITY_ID))
}
function acme::funcs::cityMapping(): acme::City[*] = {|
  #>{db.CITY}#
    -> map(r | ^acme::City(id = $r.ID, name = $r.NAME))
}

function acme::funcs::personAddressMatch(p: acme::Person[1], a: acme::Address[1]): Boolean[1] = {|
  $p.addrFk == $a.id
}
function acme::funcs::addressCityMatch(a: acme::Address[1], c: acme::City[1]): Boolean[1] = {|
  $a.cityFk == $c.id
}
```

Person → City is then `$person.address.city`. There is no `Person_City`
mapping at all; the join happens at navigation time.

**Unmodeled intermediate — you do not have to model it.** Two equivalent
options, chosen by the join's shape.

*Functional equi-join (optimization).* When the intermediate is reached by
a functional FK equality, the **source** carries the chain in a
join-derived `+local`, and the association stays a single-hop predicate
that never references the middle:

```pure
Association acme::Person_City ( Person.city [1]: City, City.persons [*]: Person )

function acme::funcs::personMapping(): acme::Person[*] = {|
  #>{db.PERSON}#
    -> join(~addr: #>{db.ADDR}#, {p, a | $p.ADDR_ID == $a.ID})
    -> map(r | ^acme::Person(id = $r.ID, name = $r.NAME, +addrCityId = $r.addr.CITY_ID))
}
function acme::funcs::cityMapping(): acme::City[*] = {|
  #>{db.CITY}#
    -> map(r | ^acme::City(id = $r.ID, name = $r.NAME))
}
function acme::funcs::personCityMatch(p: acme::Person[1], c: acme::City[1]): Boolean[1] = {|
  $p.addrCityId == $c.id
}
```

*General (non-functional / non-equi / composite).* When the intermediate
cannot collapse to a scalar key, carry it as a **physical slot** with
`join()` and reference it from a **pre-map `navigate()`** (relation space):

```pure
function acme::funcs::personMapping(): acme::Person[*] = {|
  #>{db.PERSON}#
    -> join(~addr: #>{db.ADDR}#, {p, a | $p.ADDR_ID == $a.ID})
    -> navigate(~city: acme::City.all(), {row, c | $row.addr.CITY_ID == $c.id})
    -> map(r | ^acme::Person(id = $r.ID, name = $r.NAME, city = $r.city))
}
```

ADDR is never modeled — it is a physical slot in flight. Because a pre-map
`navigate` runs in relation space, its predicate sees the carried
`$row.addr` slot. The only requirement is that the **target** key be
reachable as a class property or `+local` (`$c.id`): the right side is
object-space, the left side may be any carried physical slot.

**Clean-sheet matches legacy here.** `legacyNavigate` is just `navigate`
with a raw-table target instead of a class extent; both carry intermediate
rows as slots. So "model the middle" is a *style* recommendation, not a
capability requirement — clean-sheet expresses every legacy multi-hop
association, modeled or not. The shared structural emitter is the same
join-chain machinery class-typed property mappings already use.

**Covers:** C3 (association mapping).

### Layer 6 — M2M (structured-source, class pointers)

Source class has class-typed properties already.

```pure
function acme::funcs::staffMemberMapping(): acme::StaffMember[*] = {|
  src::Employee.all() -> map(emp | ^acme::StaffMember(
    fullName   = $emp.firstName + ' ' + $emp.lastName,
    department = ^acme::DeptInfo($emp.department)        -- cast through DeptInfo's mapping
  ))
}
```

To-many class-typed:
```pure
... -> map(emp | ^acme::StaffComplete(
        fullName = $emp.firstName + ' ' + $emp.lastName,
        projects = $emp.projects -> map(p | ^acme::ProjectInfo($p))
      ))
```

**Covers:** E2 (Pure M2M, structured source).

### Layer 6' — M2M (FK-style source)

Source has FK-like values. Identical shape to Layer 3.

```pure
function acme::funcs::personMapping(): acme::Person[*] = {|
  src::RawPerson.all()
    -> map(r | ^acme::Person(
         fullName = $r.firstName + ' ' + $r.lastName,
         firm     = navigate(acme::Firm.all(), {f | $r.firmId == $f.id})
       ))
}
```

`src::RawPerson.all()` dispatches through RawPerson's active mapping.

**Covers:** E2 (Pure M2M, FK-style source).

### Layer 7 — Aggregation

```pure
function acme::funcs::personByFirmMapping(): acme::PersonByFirm[*] = {|
  #>{db.PERSON}#
    -> groupBy(~[firmId: r | $r.FIRM_ID],
               ~[
                 headcount:   x | $x->count(),
                 totalSalary: x | $x.SALARY->sum()
               ])
    -> map(r | ^acme::PersonByFirm(
         firmId      = $r.firmId,
         headcount   = $r.headcount,
         totalSalary = $r.totalSalary
       ))
}
```

`groupBy` is just Relation API. Composes with everything.

**Covers:** B5 (~groupBy).

### Layer 8 — Inheritance

**8a. Full rewrite per subclass (canonical).** Each subclass has its
own mapping function declaring its full property set.

```pure
function acme::funcs::employeeMapping(): acme::Employee[*] = {|
  #>{db.EMPLOYEE}#
    -> map(r | ^acme::Employee(
         id        = $r.EMP_ID,
         firstName = $r.FIRST_NAME,
         lastName  = $r.LAST_NAME,
         salary    = $r.SALARY               -- subclass-specific
       ))
}

Mapping acme::M
(
  *acme::Person:                          Relational { acme::funcs::personMapping },
   acme::Employee[emp] extends [Person]:  Relational { acme::funcs::employeeMapping }
)
```

The `extends` is a binding-level annotation, not a function-level concern.

**Covers:** B7 (root), B8 (set ID), B9 (extends).

### Layer 9 — Enumeration mapping

Enumeration mapping bodies are **engine's inline static-table syntax**
— parsed as data, not as a Pure expression. The body is a finite
member-to-codes lookup table; tooling introspects the static structure
directly.

**Definition (the only supported form for now):**

```pure
Mapping acme::M
(
  ...,
  acme::Status: EnumerationMapping {
    Active:   ['A', 'ACTIVE'],
    Inactive: ['I', 'INACTIVE'],
    Pending:  ['P']
  }
)
```

**Invocation in a class mapping (function form):**

```pure
... -> map(r | ^acme::Person(
        firstName = $r.FIRST_NAME,
        status    = ^acme::Status($r.STATUS_CODE)   -- positional cast through enum mapping
      ))
```

`^acme::Status(srcExpr)` resolves to the in-scope `EnumerationMapping`
binding for `Status` and applies it. Same mental model as
`^acme::Person($row)` (Layer 4c) — *"apply the in-scope mapping for
this type."* Compile error if zero or multiple bindings for `Status`
exist in the enclosing `Mapping`.

SQL lowering: emits `CASE WHEN STATUS_CODE IN ('A','ACTIVE') THEN
'Active' ... END` directly from the static table. Equality predicates
(`$o.status == Status.Active`) inline forward to `STATUS_CODE IN
('A','ACTIVE')` — no `CASE` needed.

**🚧 Deferred — function-form enum mapping.** A future revision will
allow the binding body to be a Pure function `(S[1]) -> E[1]`
(typically built with an exhaustive value-pattern `match`) for ranges,
computed mappings, and conditional fallbacks. This requires landing
value-pattern `match` with exhaustiveness checking and adding
`Map<K,V>` operations + `^Map(...)` literal construction (the type
`meta::pure::functions::collection::Map<U,V>` is already declared as a
builtin but ships with no operations). When those land, the static
table above will desugar to a `^Map(...)` value and live alongside the
function form under a single binding contract. Until then,
EnumerationMapping accepts only the inline static-table syntax.

**Covers:** A5 (enum transformer), C4 (enumeration mapping).

---

## 7. Feature coverage matrix

Every feature from `docs/MAPPING_FEATURE_CHECKLIST.md` mapped to a
realization. **Phase 1 features** (filter, distinct, groupBy, etc.)
collapse to "use Relation API" — they're not mapping-specific.

### Property mappings

| # | Feature | Function-form realization |
|---|---|---|
| A1 | Simple column | Layer 1: `field = $r.COL` |
| A2 | Join chain + column | Layer 2 (same-store) or Layer 2' (cross-mapping `navigate`) |
| A3 | DynaFunction | Layer 1: ordinary Pure expression in the lambda |
| A4 | DynaFunction + join | Layers 1+2 combined |
| A5 | Enum transformer | Layer 9: `^Enum(srcExpr)` positional cast through the in-scope EnumerationMapping binding |
| A6 | Embedded | Layer 4a |
| A7 | Otherwise embedded | Layer 4b |
| A8 | Inline embedded | Layer 4c |
| A9 | Binding transformer (JSON) | Deferred (§9) |
| A10 | Source/Target IDs | Resolved via set-ID dispatch on `Class.all()` |
| A11 | Cross-DB reference | **Out of scope** (§8) |

### Class mapping directives

| # | Feature | Function-form realization |
|---|---|---|
| B1 | ~mainTable | Implicit — the source primitive (`tableReference`) names it |
| B2 | ~filter | Use Relation API `filter(r\|...)` in Phase 1 |
| B3 | ~filter via join | `join(...) -> filter(...)` in Phase 1 |
| B4 | ~distinct | Relation API `distinct()` |
| B5 | ~groupBy | Layer 7; Relation API `groupBy(...)` |
| B6 | ~primaryKey | Declared on the Class (not the mapping); used by lowerer for cardinality |
| B7 | Root marker | `*` prefix on Mapping binding |
| B8 | Set ID | `[setId]` suffix on Mapping binding |
| B9 | Extends | `extends [parentId]` on Mapping binding |
| B10 | Scope blocks | Not needed — function body already scopes |

### Mapping-level features

| # | Feature | Function-form realization |
|---|---|---|
| C1 | Mapping includes | `include other::M` inside Mapping |
| C2 | Store substitution | `include other::M [db1 -> db2]` inside Mapping |
| C3 | Association mapping | Layer 5 (binding to predicate function) |
| C4 | Enumeration mapping | Layer 9 (sub-construct in Mapping) |
| C5 | Local properties | `+local` declarations in `^Class(...)` |

### Database objects

| # | Feature | Function-form realization |
|---|---|---|
| D1 | Table | `#>{db.T}#` |
| D2-D5 | Joins (simple/complex/function/self) | Inline predicates in `join(...)` |
| D6 | Filter (DB-declared) | Inline in Phase 1; opt-in to named DB filters TBD |
| D7 | View | `#>{db.V}#` — no distinction from table |
| D8 | Schema | `#>{db.schema.T}#` |
| D9 | Database include | Store-level, not mapping-level |
| D10 | TabularFunction | `tableFunctionReference(...)` (open primitive) |
| D11 | MultiGrainFilter | **Out of scope** (§8) |

### Class mapping types

| # | Feature | Function-form realization |
|---|---|---|
| E1 | Relational | Default; source is `tableReference` |
| E2 | Pure (M2M) | Layers 6 / 6'; source is `Class.all()` |
| E3 | XStore | Subsumed by cross-store `navigate` |
| E4 | AggregationAware | Multiple bindings + dispatch logic; details deferred (§9) |
| E5 | Relation (`~func`) | Source is a function call — falls out of "use any Relation expression" |
| E6 | Union set | Multiple functions for same class + composing function returning `Class[*]` |
| E7 | Merge/Intersection | Same as E6 |

### Known bugs

| # | Description | Function-form status |
|---|---|---|
| F1 | Computed from 2 joins | Falls out: each `join` widens; both sub-rows available in `map` |

---

## 8. Out of scope — explicit "no"s

Catalogued features the clean-sheet design will **not** support.

| Feature | Why no |
|---|---|
| **A11** Cross-DB reference inside another DB's mapping body | Couples object meaning to two physical stores; use `navigate` |
| **Importing another project's stores/mappings into your mapping** | Tight cross-project coupling; use other project's class API (`OtherProj::Class.all()`) instead |
| **Physical joins to another class's owned tables** | The cardinal sin — couples object semantics to someone else's physical layout |
| **Arbitrary store substitution that rewrites another mapping's physical refs** | Only parametric `[db1 -> db2]` form supported |
| **D11** MultiGrainFilter | Niche; express via explicit Phase 1 filters |

---

## 9. Deliberately deferred

Known features, API surface reserved.

| Feature | Deferred because |
|---|---|
| A9 Binding transformer (JSON / external formats) | Needs Binding subsystem |
| A10 Source/Target IDs (`prop[src, tgt]`) | Sequel to B8 set ID stabilization |
| Milestoning (temporal mappings) | Significant; touches table model + lowerer |
| E3 XStore (as grammar) | Subsumed by cross-store `navigate`; grammar form indefinitely deferred |
| E4 AggregationAware dispatch | Needs alternate-mapping registration + query-shape detection |
| Named DB filter / join opt-in (`~useStoreFilter('F')`) | Convenience, low priority |
| TabularFunction primitive | Easy add, low priority |

---

## 10. Open design points

Defaults proposed; revisit before v1 freeze.

1. **Inline binding syntax.** `{| ... |}` block form for inline expressions
   in Mapping bindings. **Default:** adopt `{| ... |}` matching Pure lambda
   syntax.

2. **`navigate` overload naming.** Pre-map (Relation widen) and post-map
   (Class[*] widen) both spelled `navigate`. Distinguished by position
   and predicate signature. **Default:** keep both as `navigate`; Pure
   already has type-polymorphic primitives.

3. **Inheritance via `extends`.** When set IDs combined with `extends`,
   how multiple-table inheritance lowers. **Default:** subclass mapping
   is responsible for its full property set (Layer 8a). Sharing prep
   via §5 composition.

4. **Set ID spelling on `Class.all()`.** `Class.all()` vs
   `Class.all(~setId='emp')`. **Default:** `~setId` keyword arg.

5. **Root marker requirement.** Whether `*` is mandatory when only one
   mapping for a class exists. **Default:** required when multiple
   ClassMapping bindings exist for the same class.

6. **AssociationMapping predicate return type.** `Boolean[1]` vs an
   explicit `AssociationMatch[1]` wrapper. **Default:** `Boolean[1]`.

7. **Binding-level directives.** Whether to support per-binding
   parametric tags like `Relational ~defaultDb('myDb') { ... }`.
   **Default:** reserve the syntax space; defer implementation.

8. **`match` expression.** Pure currently has `if`-cascade only.
   Function-form spec uses `match` in enum-mapping examples for
   readability. **Default:** propose adding `match` as a Pure
   expression; until then, `if`-cascade is the implementation.

9. **Binding order inside Mapping.** Whether bindings must be grouped by
   kind or can be intermixed. **Default:** intermixed allowed; tooling
   may sort by kind for display.

10. **Inline expression body delimiters.** The kind block's `{ ... }`
    contains the body expression directly (no extra `{| ... |}` lambda
    braces). **Default:** as shown — inner braces ARE the body.

---

## 11. Relationship to current code

This doc is the **target language**, independent of:

- The current legacy declarative DSL.
- The current `MappingNormalizer` IR emission.
- Any auto-lifting from legacy to function form.

A follow-up doc (`MAPPING_LEGACY_TO_FUNCTION.md`, TBD) addresses
whether and how the legacy DSL gets bridged into this design —
including whether to keep two code paths (one declarative-data, one
function-AST) or unify them. The function form stands on its own
regardless of that decision.
