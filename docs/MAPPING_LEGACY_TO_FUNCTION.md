# Legacy Mapping DSL → Function Form

**Status:** Design spec. This document specifies how the existing
`MappingNormalizer` translates every shape of the **legacy Mapping
DSL** into the clean-sheet function form defined in
`MAPPING_CLEAN_SHEET.md`.

> **Companion docs:**
> - `MAPPING_CLEAN_SHEET.md` — the target user-facing function form.
> - `MAPPING_NORMALIZER_DESIGN.md` — historical 7-layer normalizer
>   design (legacy IR — predates this document).
> - `MAPPING_NORMALIZER_FOLLOWUPS.md` — implementation roadmap.

---

## 0. Goals & non-goals

### Goals

1. Every parseable legacy mapping desugars to **semantically
   identical** clean-sheet function form — same query results,
   same SQL.
2. The function form remains the **only** hand-written surface.
   Legacy DSL is supported for ingestion, migration, and back-compat
   only.
3. **Two parallel compiler-internal helpers** bridge the only
   shapes the clean form cannot express:
   - `legacyNavigate` — a **pipeline step**, structurally symmetric to
     clean-sheet `navigate`, used for class-typed property mappings
     whose predicate references physical columns.
   - `legacyAssocPredicate` — a **row-extraction adapter**, used inside
     AssociationMapping predicate functions when the predicate body
     references physical columns rather than class properties.
   Both are lint errors in hand-written source; the normalizer is the
   only emitter.

### Non-goals

- Bidirectional translation (function → legacy).
- Auto-codemod for hand-written legacy `.pure` files — that's a
  separate tool; this doc specifies only the *semantic* translation
  that a codemod could use.
- IR parity with legend-engine's mapping internals.

---

## 1. Strategy in one paragraph

The Parser produces a `MappingDefinition` AST (no change). The
existing `MappingNormalizer` — renamed conceptually as **the legacy
desugarer** — translates each `MappingDefinition` into realizing
Pure expressions. Most legacy shapes desugar to **pure clean-sheet
primitives** with no helper. The single shape that genuinely cannot
be expressed cleanly — class-typed navigation through physical-column
join predicates — desugars to `legacyNavigate`, which carries a
normal Pure lambda predicate but is flagged as "predicate references
physical columns, not class properties." Hand-written clean-sheet
mappings **skip the normalizer entirely**: their realizing functions
are already Pure expressions and go straight to the typechecker.

```
Legacy path:
  text → Parser → MappingDefinition → MappingNormalizer → Pure expressions → typechecker → lowerer

Clean-sheet path:
  text → Parser → Pure expressions (already function form) → typechecker → lowerer
```

---

## 2. The two helpers: `legacyNavigate` and `legacyAssocPredicate`

### Why two

Legacy DSL has two distinct shapes that genuinely cannot be expressed
in pure clean-sheet form:

1. **Class-typed property mappings whose predicate references physical
   columns** (`firm: [DB]@PersonFirm`). These produce a class instance
   for a property slot — bridged by `legacyNavigate`.
2. **AssociationMapping bodies whose predicate references physical
   columns** (`AssociationMapping(firm: [DB]@PersonFirm, ...)`). These
   return Boolean over class instances — bridged by
   `legacyAssocPredicate`.

Both helpers exist because both clean-sheet primitives they bridge
(`navigate` for class properties, AssociationMapping predicate function
for associations) are object-level. Legacy DSL writes both in physical-
column form. The two helpers provide the physical-column flavor for
each context — one returning an instance, one returning Boolean.

Every other legacy DSL shape — column refs, Embedded, Inline,
LocalProperty (`+`), DynaFunctions, M2M expressions, EnumerationMapping
tables, multi-binding enum disambiguation, `~mainTable`/`~filter`/
`~distinct`/`~groupBy`/`~primaryKey` body directives, AssociationMappings
with object-level predicates, XStore predicates, includes, store
substitution, `Relation`-kind class mappings, variant access, and join-
terminal scalar columns — desugars to pure clean-sheet primitives
(`tableReference`, `map`, `^Class`, `^Class[setId]`, `join`, `filter`,
`distinct`, `groupBy`, `+local`, `otherwise`, positional
casts) with no helper. See §5. (`~primaryKey` is parsed but not lowered —
it is graph-fetch identity metadata in the engine, not a query operator;
see §5.3.6.)

### 2.1 `legacyNavigate` — pipeline step, symmetric to `navigate`

#### Signature (pipeline step)

`legacyNavigate` is a Relation pipeline step, structurally identical
to clean-sheet `navigate`. It widens the current row scope by adding a
named slot bound to an instance of the target class, materialized
through the target's mapping.

```pure
function meta::legacy::legacyNavigate<T>(
    rowScope:  Relation[1],
    ~slot:     Class<T>[1],
    predicate: Function<{Row[1], Row[1]} → Boolean[1]>[1]
): Relation[1]   -- the input scope, widened with the named slot
```

(The result's slot has multiplicity matching the corresponding
property declaration: `[1]`, `[0..1]`, or `[*]`.)

- `~slot`: the target class (with optional set-ID suffix for multi-
  binding cases, e.g., `acme::Address[fallbackSet]`).
- `predicate`: a normal Pure lambda taking two **row references**
  (source-row from the current scope, target-main-table-row of the
  slot class) and returning `Boolean[1]`. The lambda body uses
  physical-column access (`$srcRow.COL`, `$tgtRow.COL`) on both sides
  — this is the only thing that makes it "legacy" rather than clean.

#### Symmetry with clean-sheet `navigate`

```pure
-- Clean-sheet (object-level lambda):
... -> navigate(~firm: acme::Firm.all(), {p, f | $p.firmId == $f.id})

-- Legacy bridge (physical-row lambda):
... -> legacyNavigate(~firm: acme::Firm,    {srcRow, tgtRow | $srcRow.FIRM_ID == $tgtRow.ID})
```

Identical pipeline shape, identical slot binding, identical downstream
semantics. Only the lambda language differs. This symmetry is
deliberate: a future codemod migrating a property to clean form is a
single-token replacement of `legacyNavigate` → `navigate` plus a
lambda-body rewrite — the surrounding pipeline structure stays
identical.

#### Single-hop example

Legacy DSL `firm: [DB]@PersonFirm` (where the Database declares
`Join PersonFirm(T_PERSON.FIRM_ID = T_FIRM.ID)`) becomes:

```pure
function acme::funcs::personMapping(): acme::Person[*] = {|
  tableReference('DB', 'T_PERSON')
    -> legacyNavigate(~firm: acme::Firm, {srcRow, tgtRow | $srcRow.FIRM_ID == $tgtRow.ID})
    -> map(r | ^acme::Person(firstName = $r.FIRST_NAME, firm = $r.firm))
|}
```

#### Composability

Because `legacyNavigate` produces a class instance into a named slot
in the row scope (or, when used as the value of a `map` field, returns
an instance directly), any clean-sheet primitive that consumes a class
instance accepts it interchangeably with `navigate`.

For `otherwise(partial, fallback)`:

```pure
-- Inside the map(r | ^acme::Person(... )):
address = otherwise(
  ^acme::Address(street = $r.STREET, city = $r.CITY),    -- partial ctor (clean)
  legacyNavigate(                                          -- legacy fallback
    ~slot: acme::Address[fallbackSet],
    {srcRow, tgtRow | $srcRow.ADDR_ID == $tgtRow.ID}
  )
)
```

After a future codemod, only the fallback expression evolves:

```pure
address = otherwise(
  ^acme::Address(street = $r.STREET, city = $r.CITY, +addressId = $r.ADDR_ID),
  navigate(~slot: acme::Address[fallbackSet], (p, a) | $p.addressId == $a.id)
)
```

### 2.2 `legacyAssocPredicate` — row-extraction adapter for AssociationMappings

#### Signature

```pure
function meta::legacy::legacyAssocPredicate<A, B>(
    a: A[1],
    b: B[1],
    predicate: Function<{Row[1], Row[1]} → Boolean[1]>[1]
): Boolean[1]
```

- `a, b`: the two **class instances** that the AssociationMapping
  predicate function takes as parameters.
- `predicate`: a lambda over the **underlying main-table rows** of
  those instances, returning `Boolean[1]`.

The helper's runtime semantics: extract the main-table rows of `$a`
and `$b` (using their classes' mappings to locate `~mainTable`), bind
them to the lambda's `$srcRow` and `$tgtRow` parameters, and evaluate.
At SQL lowering time it produces the natural column comparisons.

#### Why it exists

An AssociationMapping is always a function `(A[1], B[1]) -> Boolean[1]`
— in both clean-sheet and legacy. In clean-sheet, the function body
reads class properties directly (`$a.x == $b.y`). In legacy, the
predicate uses physical columns of `A`'s and `B`'s main tables, but
the function parameters are still class instances. `legacyAssocPredicate`
is the adapter that lets the lambda body speak the row-level scope
while the surrounding function speaks the instance-level scope.

#### Example

Legacy `acme::Person_Firm: Relational { AssociationMapping(firm: [DB]@PersonFirm, ...) }`
becomes:

```pure
function acme::funcs::personFirmMatch(p: acme::Person[1], f: acme::Firm[1]): Boolean[1] = {|
  legacyAssocPredicate(
    $p, $f,
    {srcRow, tgtRow | $srcRow.FIRM_ID == $tgtRow.ID}
  )
|}

Mapping acme::M ( acme::Person_Firm: AssociationMapping { acme::funcs::personFirmMatch } )
```

The outer function signature is identical to a clean AssociationMapping
predicate function; only the body uses the legacy adapter.

### 2.3 Lint rule (hand-written source)

One rule covers both helpers.

```
ERR LEG001: 'legacyNavigate' and 'legacyAssocPredicate' are
            compiler-internal helpers. Use 'navigate(...)' or an
            object-level predicate function instead. If the
            source/target columns aren't modeled as class properties,
            add them via '+local' on the binding, then write the
            predicate in object terms.
```

The parser tags AST nodes with provenance; the lint rejects any call
with `userSource` provenance. The MappingNormalizer constructs both
helpers programmatically — they carry `compilerSynthesized` provenance
and never trigger the lint.

---

## 3. Pipeline placement

```
Parser
  → MappingNormalizer  (legacy MappingDefinition → Pure expressions)
  → PureModelBuilder
  → TypeChecker
  → MappingResolver
  → Lowerer
  → Dialect
```

The MappingNormalizer phase runs **only for legacy mappings**. Hand-
written clean-sheet mappings — where every binding directly names a
realizing function — bypass it. The phase is structurally identical
to today's `MappingNormalizer.java`; only its internal rules change
to match this document.

### Includes are resolved upstream

`Database X ( include Y )` and `Mapping X ( include Y )` are
resolved by the parser/resolver during binding-table flattening,
**before** the MappingNormalizer runs. By the time the normalizer
sees a `MappingDefinition`, the binding table is complete; included
bindings appear in the same shape as locally declared ones.

The only desugar-time action related to includes is **store
substitution** (`include Y [store::A -> store::B]`), which is a
token-level rewrite of `tableReference('store::A', ...)` to
`tableReference('store::B', ...)` applied to the included bindings
as they're flattened into the parent. No IR helper is exposed for
this.

---

## 4. Reading guide for §5

Each subsection has the shape:

```
### N. Legacy shape: <name>

**Legacy DSL:**
  <example>

**Function form (desugared):**
  <example>

**Transformation rule:**
  <mechanical rewrite>

**Notes:**
  - ...
```

Where `legacyNavigate` appears in the desugared form, it's marked
with a comment.

---

## 5. Per-shape transformations

### Section index

- **5.1** Mapping shell, includes, store substitution
- **5.2** Class mapping prefixes
- **5.3** Body directives (`~mainTable`, `~filter`, `~distinct`,
  `~groupBy`, `~primaryKey`)
- **5.4** Property mappings (11 variants)
- **5.5** Pure (M2M) class mappings
- **5.6** Association mappings (Relational + XStore)
- **5.7** Enumeration mappings

---

## 5.1 Mapping shell, includes, store substitution

### 5.1.1 Bare Mapping container

**Legacy DSL:**

```pure
Mapping acme::M
(
  ... bindings ...
)
```

**Function form:** identity. The `Mapping fqn ( ... )` container is
the clean-sheet shell already.

---

### 5.1.2 `include` (within-project or cross-project)

**Legacy DSL:**

```pure
Mapping acme::M
(
  include other::BaseMapping
  acme::Foo: Relational { ... }
)
```

**Function form (desugared):** the include directive disappears.
The bindings from `other::BaseMapping` are flattened into `acme::M`'s
binding table by the parser/resolver. From the normalizer's
perspective, those bindings look exactly like locally declared
ones. Each is desugared independently using the rules below.

**Transformation rule:**

1. Parser/resolver loads `other::BaseMapping` (transitively if it
   itself uses include).
2. For each binding `B` in `other::BaseMapping` not overridden in
   `acme::M`, copy `B` into `acme::M`'s effective binding table.
3. Local bindings in `acme::M` take precedence over included
   bindings for the same `(class, setId)` key.
4. The MappingNormalizer runs over the flattened binding table; no
   IR-level include directive remains.

**Cross-project includes** are subject to the normal project-
dependency graph — same rule as any cross-project Pure import. If
the dependency is permitted, the include resolves; if not, the
parser/resolver rejects it with a project-graph error.

**Notes:**

- Include is a **legacy-only artifact**. In clean-sheet, cross-
  project navigation works via object-level Associations between
  classes mapped in separate projects, with mapping composition
  happening at the `Runtime` level (a Runtime lists multiple
  Mappings).
- Include exists in legacy because legacy property mappings
  reference physical columns of other classes' tables (the
  cardinal sin), which requires those classes' bindings to be in
  scope for resolution. Once `legacyNavigate` is the only such
  reference, the requirement is satisfied by include's existing
  parse-time flattening.

---

### 5.1.3 `include` with store substitution

**Legacy DSL:**

```pure
Mapping acme::ProdM
(
  include other::DevMapping [store::DevDB -> store::ProdDB]
  ...
)
```

**Function form (desugared):** as 5.1.2, but with substitution.

**Transformation rule:**

1. Load `other::DevMapping`.
2. For each binding `B` included:
   - Walk `B`'s realizing expressions and rewrite every
     `tableReference('store::DevDB', T)` to
     `tableReference('store::ProdDB', T)`.
   - Rewrite any embedded join references that name `store::DevDB`
     similarly.
3. Insert the rewritten `B` into `acme::ProdM`'s effective binding
   table.

**Notes:**

- This is a legacy-only mechanism. In clean-sheet, the same QA/PROD
  pattern is achieved by **parameterizing realizing functions over
  store name** and passing the store at the binding site:
  ```pure
  Mapping acme::ProdM ( acme::Person: Relational { other::funcs::personMapping('store::ProdDB') } )
  Mapping acme::QaM   ( acme::Person: Relational { other::funcs::personMapping('store::QaDB')   } )
  ```
  No directive, no implicit rewrite — just function argument
  passing.

---

### 5.1.4 Database include

**Legacy DSL:**

```pure
Database trading::TradingDB ( include refdata::RefDB, Table TRADE (...) )
```

**Function form:** also resolved at parse time. The merged Database
makes `refdata::RefDB`'s tables visible inside `trading::TradingDB`'s
namespace, which allows legacy joins like
`Join Trade_Sector(TRADE.SECTOR_CODE = SECTOR.CODE)` to be declared
and parsed.

**Notes:**

- Database include exists **exclusively** to support legacy cross-
  database physical joins (the cardinal sin at the store layer).
  In clean-sheet, cross-store edges are object-level Associations
  between classes mapped in different stores; no Database
  composition is needed and the normalizer never sees Database
  include as a separate concern.
- The MappingNormalizer doesn't care about Database include — by
  the time it runs, the table namespace is already flat.

---

### 5.1.5 `testSuites`

**Legacy DSL:**

```pure
Mapping acme::M ( ..., testSuites: [ Suite1: { ... } ] )
```

**Function form:** copy-through unchanged. Test suites aren't part
of runtime semantics; the normalizer preserves them verbatim.

---

## 5.2 Class mapping prefixes

### 5.2.1 Root marker `*`

```pure
*acme::Person: Relational { ... }
```

→ preserved verbatim on the function-form binding.

### 5.2.2 Set ID `[setId]`

```pure
acme::Person[emp]: Relational { ... }
```

→ preserved verbatim on the function-form binding.

### 5.2.3 `extends [parentSetId]`

```pure
acme::Employee[emp] extends [Person]: Relational { ... }
```

**Function form:** the child binding's realizing function absorbs
all inherited property mappings from the parent (resolved by
`setId`), with child PMs overriding on conflict.

**Transformation rule:**

1. Resolve parent binding by `setId`.
2. Concatenate parent + child property mappings (child wins).
3. Generate child's realizing function with the merged PM set.
4. Preserve the `extends [parent]` annotation on the binding for
   query-time set-ID dispatch.

**Notes:**

- Multi-level `extends` resolves recursively before flattening.
- The parent's `~mainTable` is **not** auto-copied; the child must
  declare its own. (Engine semantics here are inconsistent; the
  function form requires explicitness.)

---

## 5.3 Class mapping body directives

These appear in a Relational class mapping body and become
**pipeline steps in the realizing function**.

### 5.3.1 `~mainTable`

```pure
acme::Person: Relational { ~mainTable [store::DB] T_PERSON, ... }
```

→

```pure
function acme::funcs::personMapping(): acme::Person[*] = {|
  tableReference('store::DB', 'T_PERSON')
    -> map(r | ^acme::Person(<fields from PMs>))
|}
```

- `[DB] SCHEMA.TABLE` (dotted) → `tableReference('DB', 'SCHEMA.TABLE')`.

### 5.3.2 `~filter` (direct)

```pure
~filter [DB] activeFilter
```

where the Database declares `Filter activeFilter(T_PERSON.STATUS = 'A')`.

→ the desugarer **inlines the named filter's predicate expression**
directly into a clean-sheet `filter` step:

```pure
-> filter(r | $r.STATUS == 'A')
```

**No helper.** Named filters are a Database-declared predicate; the
desugarer resolves the filter by name at desugar time and substitutes
its body into the pipeline.

### 5.3.3 `~filter` (join-mediated)

```pure
~filter [DB]@J1 > @J2 | [DB] usOnlyFilter
```

→ join cascade + inlined filter predicate:

```pure
tableReference('DB', 'T_PERSON')
  -> join(~addr:    tableReference('DB', 'T_ADDR'),    {p, a | $p.ADDR_ID == $a.ID})
  -> join(~country: tableReference('DB', 'T_COUNTRY'), {r, c | $r.addr.COUNTRY_ID == $c.ID})
  -> filter(r | $r.country.CODE == 'US')   -- inlined from `usOnlyFilter`
  -> map(r | ^acme::Person(<fields>))
```

Named Join predicates and named Filter predicates are both inlined.
**No helper** — within-store widening at the Relation level (clean-
sheet Layer 2).

### 5.3.4 `~distinct`

```pure
~distinct
```

→ `-> distinct()` in the pipeline.

### 5.3.5 `~groupBy`

```pure
~groupBy(POSITIONS.ACCOUNT_ID, POSITIONS.ASSET_ID)
quantity: sum([DB] POSITIONS.QUANTITY)
```

→

```pure
tableReference('DB', 'POSITIONS')
  -> groupBy(~[ACCOUNT_ID, ASSET_ID],
             ~[QUANTITY: row | $row.QUANTITY -> sum()])
  -> map(r | ^acme::Position(quantity = $r.QUANTITY, ...))
```

The desugarer classifies each PM as grouping-key or aggregate
based on whether its expression matches a `~groupBy` argument or is
wrapped in `sum`/`count`/etc.

### 5.3.6 `~primaryKey`

```pure
~primaryKey(ADDRESS.ID)
```

→ **parsed but not lowered** (no pipeline step). In the engine, `~primaryKey`
is *object-identity metadata*, not a query operator: it is stored on the
`RootRelationalInstanceSetImplementation` and consumed at **graph-fetch** time
(PK columns / getters to correlate result rows to objects and dedup/merge
object graphs across set implementations), and is defaulted from `~groupBy`
columns or the underlying table/view declared primary key when absent
(`HelperRelationalBuilder.processRelationalClassMapping` /
`processRelationalPrimaryKey`). The engine never emits a row-level
`distinct`/`distinctBy` for it. legend-lite has no graph-fetch consumer yet,
so the normalizer keeps `~primaryKey` on the parsed `ClassMapping` but emits
no step in the realizing function. (Lowering it to a `distinctBy` would
conflate identity with row dedup and diverge from engine semantics.)

---

## 5.4 Property mappings — 11 variants

### 5.4.1 Simple Column

```pure
firstName: [DB] T_PERSON.FIRST_NAME
```

→ `firstName = $r.FIRST_NAME` (the DB qualifier is dropped; we're
already inside `tableReference('DB', 'T_PERSON')`).

---

### 5.4.2 Column with EnumerationMapping reference

```pure
status: EnumerationMapping legacyStatus : [DB] T_ORDER.STATUS_CODE
```

→

```pure
status = acme::M.legacyStatus($r.STATUS_CODE)
```

When the enum has multiple bindings in the same Mapping, the PM
names one explicitly. Desugars to a **set-ID-qualified cast**:
`MappingFqn.bindingSetId(srcExpr)`. When there's exactly one
binding, the simpler form `^acme::Status($r.STATUS_CODE)` is emitted
(no set-ID needed).

**No helper.** Reuses the multi-binding cast syntax that already
applies to class mappings (`^Class[setId]($src)`).

---

### 5.4.3 Class-typed Join — single-hop

```pure
firm: [DB]@PersonFirm
```

where `firm: acme::Firm[1]` is class-typed and `@PersonFirm` is
`Join PersonFirm(T_PERSON.FIRM_ID = T_FIRM.ID)`.

→ `legacyNavigate` is added as a **pipeline step** widening the row
scope; the final `map` reads `$r.firm` to materialize the property:

```pure
function acme::funcs::personMapping(): acme::Person[*] = {|
  tableReference('DB', 'T_PERSON')
    -> legacyNavigate(~firm: acme::Firm, {srcRow, tgtRow | $srcRow.FIRM_ID == $tgtRow.ID})
    -> map(r | ^acme::Person(firstName = $r.FIRST_NAME, firm = $r.firm))
|}
```

Structurally identical to clean-sheet `navigate`; only the lambda is
physical-row. The lowerer resolves `~slot: acme::Firm` to find Firm's
main-table binding (via Firm's mapping) and compiles the lambda to
SQL `JOIN ON`.

---

### 5.4.4 Class-typed Join — multi-hop

```pure
country: [DB]@PersonAddress > @AddressCountry
```

→ a **chain of clean-sheet `join` steps** binding each intermediate
physical row as a named slot, followed by a **single `legacyNavigate`
step** that materializes the final target class:

```pure
function acme::funcs::personMapping(): acme::Person[*] = {|
  tableReference('DB', 'T_PERSON')
    -> join(~addr: tableReference('DB', 'T_ADDR'),
            {p, a | $p.ADDR_ID == $a.ID})                               -- intermediate physical row
    -> legacyNavigate(~country: acme::Country,
                      {r, c | $r.addr.COUNTRY_ID == $c.ID})              -- final class via mapping
    -> map(r | ^acme::Person(firstName = $r.FIRST_NAME, country = $r.country))
|}
```

Trace through it:

1. Start from `T_PERSON` rows.
2. Intermediate hop `T_ADDR` is just a physical row — no class involved.
   Use clean-sheet `join` to bind it as named slot `~addr`.
3. Final hop `T_COUNTRY` materializes the `acme::Country` class through
   its own mapping. Use `legacyNavigate` to bind it as named slot
   `~country`. The predicate reads `$r.addr.COUNTRY_ID` (chained dot
   access into the intermediate slot) and `$c.ID` (target row).
4. The final `map` constructs Person with `country = $r.country`.

**No `exists` quantifier.** Intermediate row binding is handled by the
pipeline structure itself (clean-sheet `join` adds named slots that are
dot-accessible in subsequent steps). `legacyNavigate` only appears at
the final hop because that's where we cross from rows to a class
instance.

#### Symmetric with clean-sheet multi-hop

```pure
-- Clean-sheet (object lambdas, navigate chain):
... -> navigate(~addr:    acme::Address.all(), {p, a | $p.addressId == $a.id})
   -> navigate(~country:  acme::Country.all(), {r, c | $r.addr.countryId == $c.id})
   -> map(r | ^Person(country = $r.country))

-- Legacy bridge (physical-row lambdas, join chain + final legacyNavigate):
... -> join(~addr:           tableReference('DB', 'T_ADDR'), {p, a | $p.ADDR_ID == $a.ID})
   -> legacyNavigate(~country: acme::Country,                {r, c | $r.addr.COUNTRY_ID == $c.ID})
   -> map(r | ^Person(country = $r.country))
```

Identical pipeline shape; only the lambda language differs at each
hop. The codemod is a token-level replacement per hop.

**Notes:**

- Multi-hop joins must traverse a **single store**; cross-store
  chains are a hard reject (see §6.1).
- Intermediate hops use clean-sheet `join` because they bind physical
  rows, not class instances. `legacyNavigate` is used only at the
  final hop that crosses to a class.
- For ≥3 hops the pattern extends naturally: `join → join → ... →
  legacyNavigate`.

---

### 5.4.5 Join-terminal column

```pure
countryName: [DB]@PersonAddress > @AddressCountry | T_COUNTRY.NAME
```

This is **scalar** (the property is a primitive), not class-typed.

→

```pure
-- desugars in the realizing function body, BEFORE the map step:
tableReference('DB', 'T_PERSON')
  -> join(JoinType.INNER, tableReference('DB', 'T_ADDR'), (p, a) | $p.ADDR_ID == $a.ID)
  -> join(JoinType.INNER, tableReference('DB', 'T_COUNTRY'), (a, c) | $a.COUNTRY_ID == $c.ID)
  -> map(r | ^acme::Person(..., countryName = $r.NAME))
```

**No helper needed.** Clean-sheet `join(...)` cascade + `$r.NAME`
column access in the final `map`.

The desugarer **merges** overlapping join chains across PMs on the
same binding into a single `join` cascade (no duplicate joins).

---

### 5.4.6 Expression (DynaFunction)

```pure
fullName: concat([DB] T_PERSON.FIRST_NAME, ' ', [DB] T_PERSON.LAST_NAME)
```

→ `fullName = $r.FIRST_NAME + ' ' + $r.LAST_NAME`

The desugarer walks the `RelationalOperation` expression tree and
rewrites:

- `[DB] T.COL` → `$r.COL`.
- `[DB]@J | T.COL` → join-cascade contribution (as in 5.4.5).
- Function-name translation table (`concat → +`, `equal → ==`,
  `lessThan → <`, ...).

**No helper.** If the expression references multiple join chains,
each chain becomes a join contribution in the surrounding pipeline.

---

### 5.4.7 Embedded

```pure
address (
  street: [DB] T_PERSON.STREET,
  city:   [DB] T_PERSON.CITY
)
```

→ inline `^acme::Address(...)` ctor assigned to the parent's class-
typed property:

```pure
address = ^acme::Address(street = $r.STREET, city = $r.CITY)
```

Sub-PMs recurse through the same rules. Embedded shares the parent's
table — no separate `tableReference`. Multi-level embedded nests
cleanly. **No helper.** Matches `MAPPING_CLEAN_SHEET.md` Layer 4a.

---

### 5.4.8 Inline embedded

```pure
address() Inline[addressSet]
```

→ positional cast through the target's mapping:

```pure
address = ^acme::Address[addressSet]($r)
```

If the target class has exactly one binding, `[addressSet]` is
dropped. **No helper.** Matches Layer 4c.

---

### 5.4.9 OtherwiseEmbedded

```pure
address (
  street: [DB] T_PERSON.STREET,
  city:   [DB] T_PERSON.CITY
)
Otherwise([fallbackSet] : [DB]@PersonAddress)
```

The fallback is structurally a class-typed Join PM, so it desugars the
same way as 5.4.3: pipeline `legacyNavigate` step binding a named slot,
plus `otherwise` composition in the final `map`:

```pure
function acme::funcs::personMapping(): acme::Person[*] = {|
  tableReference('DB', 'T_PERSON')
    -> legacyNavigate(
         ~addrFallback: acme::Address[fallbackSet],
         {srcRow, tgtRow | $srcRow.ADDR_ID == $tgtRow.ID}
       )
    -> map(r | ^acme::Person(
         address = otherwise(
           ^acme::Address(street = $r.STREET, city = $r.CITY),   -- partial ctor
           $r.addrFallback                                         -- fallback slot
         )
       ))
|}
```

**Composition:** `otherwise(partial, fallback)` takes a partial
`^Class(...)` ctor and a fallback instance. The fallback can be any
expression of the slot's class type — `$r.addrFallback` (from a
`legacyNavigate` step), `$r.addr` (from a clean `navigate` step), or a
direct instance value. The codemod replaces only the source of the
fallback slot; the `otherwise` call stays identical.

---

### 5.4.10 LocalProperty (`+`)

```pure
+firmId: String[1] : [DB] T_PERSON.FIRM_ID
```

→ `+local` named argument in the constructor:

```pure
^acme::Person(..., +firmId = $r.FIRM_ID)
```

The type annotation is preserved if present; otherwise inferred
from the RHS. **No helper.** Matches `MAPPING_CLEAN_SHEET.md` §4.1.

---

### 5.4.11 Arrow-chain (variant access on column)

```pure
priceUSD: [DB] T_PAYLOAD.PAYLOAD->get('priceUSD', @Float)
```

→ `priceUSD = $r.PAYLOAD->get('priceUSD', @Float)`

The arrow chain is captured by the parser as a structured
expression and emitted verbatim into the realizing function. **No
helper** — `->get(...)` is a normal Pure expression already; the
legacy parser's raw-text capture is just a parser-side
implementation detail that doesn't affect the desugar.

**Notes:**

- This shape is a lite-engine extension (JSON payload columns), not
  a legend-engine concept. No legacy bridge needed for engine-
  parity reasons.

---

## 5.5 Pure (M2M) and Relation class mappings

### 5.5.1 Basic M2M

```pure
Person: Pure {
  ~src raw::RawPerson
  fullName: $src.firstName + ' ' + $src.lastName
}
```

→

```pure
function acme::funcs::personM2M(): acme::Person[*] = {|
  raw::RawPerson.all()
    -> map(src | ^acme::Person(fullName = $src.firstName + ' ' + $src.lastName))
|}
```

- `~src SourceClass` → `SourceClass.all()` as Phase-1 source.
- Each PM (already a Pure expression in legacy) becomes a field
  assignment in `^Class(...)`.

### 5.5.2 M2M with `~filter`

```pure
ActivePerson: Pure {
  ~src raw::RawPerson
  ~filter $src.isActive == true
  firstName: $src.firstName
}
```

→ `-> filter(src | $src.isActive == true)` step before `map`.

### 5.5.3 M2M with class-typed property (chained)

```pure
PersonWithAddress: Pure {
  ~src raw::RawPerson
  address: $src.rawAddress     -- target is mapped elsewhere
}
```

→ `address = ^acme::Address($src.rawAddress)` — positional cast
through Address's mapping (Layer 6). **No helper.**

### 5.5.4 M2M with multi-valued class-typed property

```pure
addresses: $src.rawAddresses    -- [*]
```

→ `addresses = $src.rawAddresses -> map(a | ^acme::Address($a))`.

### 5.5.5 `Relation`-kind class mapping

Legacy DSL also supports a `Relation` kind, where the source is a
function returning a Relation rather than a table:

```pure
acme::Person: Relation
{
  ~src acme::funcs::personRelation
  ~filter $row.STATUS == 'A'
  firstName: $row.FIRST_NAME,
  lastName:  $row.LAST_NAME
}
```

→ trivially: invoke the Relation-valued function, then `filter` and
`map` exactly as for `~mainTable`-based Relational mappings:

```pure
function acme::funcs::personMapping(): acme::Person[*] = {|
  acme::funcs::personRelation()
    -> filter(row | $row.STATUS == 'A')
    -> map(row | ^acme::Person(firstName = $row.FIRST_NAME, lastName = $row.LAST_NAME))
|}
```

**No helper.** The clean-sheet `Relational` binding already lets the
realizing function start from any Relation expression — `Relation`-kind
is just a syntactic affordance for declaring "the source isn't a
table." Body directives (`~filter`, `~distinct`, `~groupBy`,
`~primaryKey`) and property mappings work identically.

---

## 5.6 Association mappings

### 5.6.1 Relational AssociationMapping

```pure
acme::Person_Firm: Relational
{
  AssociationMapping (
    firm:      [DB]@PersonFirm,
    employees: [DB]@PersonFirm
  )
}
```

→ a **predicate function** of type `(A[1], B[1]) -> Boolean[1]` whose
body uses `legacyAssocPredicate` to express the physical-column
predicate against the underlying main-table rows:

```pure
function acme::funcs::personFirmMatch(p: acme::Person[1], f: acme::Firm[1]): Boolean[1] = {|
  legacyAssocPredicate(
    $p, $f,
    {srcRow, tgtRow | $srcRow.FIRM_ID == $tgtRow.ID}
  )
|}

Mapping acme::M
(
  acme::Person_Firm: AssociationMapping { acme::funcs::personFirmMatch }
)
```

The outer function signature is identical to a clean AssociationMapping
predicate function — `(A[1], B[1]) -> Boolean[1]`. Only the body
uses the `legacyAssocPredicate` adapter to bridge from the instance-
level parameter scope to the row-level lambda scope.

#### Why `legacyAssocPredicate` (not `+local` synthesis)

The legacy DSL writes the predicate as `T_PERSON.FIRM_ID = T_FIRM.ID`
— physical columns on both sides. The clean AssociationMapping form
wants object terms (`$p.firmId == $f.id`). One could synthesize a
`+local firmId` field on Person to make the object-level predicate
typecheck, but that mutates Person's effective property surface in a
way that's not localized to this AssociationMapping. The
`legacyAssocPredicate` adapter is the cleaner bridge: it preserves
the physical-column form verbatim and leaves the class's property
surface untouched.

This treatment is **consistent with class-typed property mappings**
(§5.4.3), where we use `legacyNavigate` rather than synthesizing
`+local`. Both legacy bridges keep physical-column predicates as
physical-column lambdas; codemod migration is responsible for lifting
to object terms when desired.

#### Codemod target

A future codemod replaces:

```pure
function acme::funcs::personFirmMatch(p, f): Boolean[1] = {|
  legacyAssocPredicate($p, $f, {srcRow, tgtRow | $srcRow.FIRM_ID == $tgtRow.ID})
|}
```

with:

```pure
function acme::funcs::personFirmMatch(p, f): Boolean[1] = {|
  $p.firmId == $f.id
|}
```

plus (if needed) adding `firmId` as a class property or `+local` on
Person's binding. The Mapping's AssociationMapping declaration is
untouched — same function name, same signature.

---

### 5.6.1b Multi-hop AssociationMapping (per-end navigation)

```pure
acme::Person_City: Relational
{
  AssociationMapping (
    city:    [DB] @PersonAddress > @AddressCity,
    persons: [DB] @AddressCity   > @PersonAddress
  )
}
```

A `legacyAssocPredicate` predicate function **cannot** express this: it
receives only the two class instances `($p, $c)` and has no handle on the
intermediate `ADDR` row, which exists only inside a realizing function's
pipeline (relation space). See §5.6.3.

**Rule (Option A).** A multi-hop association end desugars to a **class-typed
property mapping on the end's owning class**, emitted through the **same
join-chain machinery as a class-typed Join PM** (§5.4.4) — `join` for
intermediate hops, `legacyNavigate` at the terminus. There is **no
standalone predicate function** for the multi-hop case; the navigation
lives in each end's class realizing function:

```pure
function acme::M_Person(): acme::Person[*] = {|
  tableReference('DB','T_PERSON')
    -> join(~PersonAddress: tableReference('DB','T_ADDR'),
            {s, t | $s.ADDR_ID == $t.ID})
    -> legacyNavigate(~city: acme::City.all(),
            {addrRow, cityRow | $addrRow.CITY_ID == $cityRow.ID})
    -> map(row | ^acme::Person(id = $row.ID, city = $row.city))
}
function acme::M_City(): acme::City[*] = {|
  tableReference('DB','T_CITY')
    -> join(~AddressCity: tableReference('DB','T_ADDR'),
            {s, t | $s.ID == $t.CITY_ID})
    -> legacyNavigate(~persons: acme::Person.all(),
            {addrRow, personRow | $addrRow.ADDR_ID == $personRow.ID})
    -> map(row | ^acme::City(id = $row.ID, persons = $row.persons))
}
```

This is **exactly** what the old engine-module normalizer did
(`addAssociationExtends`: a pre-pass scanning each class's association
navigations and appending `extend(~prop: traverse(...))`), but emitting the
clean-sheet `join`/`legacyNavigate` primitives instead of relational
`extend`/`traverse`. Multi-hop rides the hop **list** natively — no
`(A,B)->Boolean` quantifier problem.

**Implementation shape:**

- **Pre-pass** (before class-mapping synthesis): for each multi-hop
  `AssociationMapping` end, determine the owning class (the *opposite* end's
  target class in the `Association` declaration) and inject a
  `PropertyMapping.Join(propName, db, joins)` into that class's
  `ClassMapping.Relational.propertyMappings`.
- **Association-aware property resolution:** `findPropertyTypeDeep` must
  also resolve **association properties** (not just declared class +
  superclass properties), so the injected PM passes `validatePmNames` and
  `classTypedTargetIfMapped` resolves the terminus class for the final-hop
  `legacyNavigate`. Association properties are class properties
  semantically; this is the correct resolution, not a special case.
- **Single-hop ends** keep the §5.6.1 standalone `legacyAssocPredicate`
  predicate (the symmetric-predicate optimization). Multi-hop ends produce
  no predicate function.

#### 5.6.3 Why not a predicate for multi-hop

The predicate form `(A[1], B[1]) -> Boolean[1]` is the join condition
between two extents. For single-hop it only needs columns from `A`'s and
`B`'s tables — both recoverable from the two instances' backing rows
(`legacyAssocPredicate`). Multi-hop needs a column from a **third
(intermediate) table**, but the predicate never receives that row. The
intermediate row exists only inside a realizing function's pipeline, so the
navigation must be emitted **there** — as a per-end class-typed property
mapping — not as a 2-instance predicate. (Clean-sheet has the identical
shape with `navigate`; see `MAPPING_CLEAN_SHEET.md` Layer 5.)

---

### 5.6.2 XStore association mapping

```pure
acme::Person_Firm: XStore
{
  firm:      $this.firmId == $that.id,
  employees: $this.id     == $that.firmId
}
```

→ same as 5.6.1 — a predicate function over class instances:

```pure
function acme::funcs::personFirmMatch(p: acme::Person[1], f: acme::Firm[1]): Boolean[1] = {|
  $p.firmId == $f.id
|}
```

XStore predicates are already object-level (`$this`/`$that` are
class instances, not rows). The desugarer just rewrites `$this`/
`$that` to the lambda parameter names. **No helper.**

XStore is the legacy "object-level cross-store association" — in
clean-sheet, every AssociationMapping is object-level, so XStore
collapses into the same shape as 5.6.1.

---

## 5.7 Enumeration mappings

### 5.7.1 Basic enumeration mapping

```pure
acme::Status: EnumerationMapping
{
  Active:   ['A', 'ACTIVE'],
  Inactive: ['I']
}
```

→ identity. The clean-sheet form uses the same inline static-table
syntax (per `MAPPING_CLEAN_SHEET.md` Layer 9).

### 5.7.2 Multi-binding (id-disambiguated)

```pure
acme::Status: EnumerationMapping primaryStatus { Active: ['A'], ... }
acme::Status: EnumerationMapping legacyStatus  { Active: ['1'], ... }
```

→ each becomes a `[setId]`-annotated binding, mirroring the
class-mapping set-ID convention:

```pure
Mapping acme::M
(
  acme::Status[primaryStatus]: EnumerationMapping { Active: ['A'], ... },
  acme::Status[legacyStatus]:  EnumerationMapping { Active: ['1'], ... }
)
```

The `id` in legacy becomes a `[setId]`. PMs that reference the
binding use the set-ID cast (see 5.4.2).

### 5.7.3 Other code shapes

Integer codes, enum-typed source codes (`LegacyStatus.NEW`),
mixed-type lists: identity. The clean-sheet form accepts the same
literal syntax.

---

## 6. Hard "no"s

The MappingNormalizer rejects these legacy shapes with clear errors;
migration requires source changes.

### 6.1 Cross-store physical joins in a class mapping

```pure
firm: [store::DB1]@PersonFirm > [store::DB2]@FirmDetail
```

A property-mapping join chain that spans stores. **Hard reject.**

```
ERR LEG002: cross-store join chain in property mapping is not supported.
            Property 'firm' on class 'acme::Person' joins from
            'store::DB1' to 'store::DB2'. Cross-store edges must be
            modeled as object-level AssociationMappings between the
            two classes, not as join chains in a class mapping.
            See docs/MAPPING_LEGACY_TO_FUNCTION.md §6.1.
```

**Workaround:** declare an Association between the two classes and
write an AssociationMapping predicate over their class properties.
The lowerer federates at SQL generation time.

### 6.2 `AggregationAware` class-mapping kind

Legacy engine supports an `AggregationAware` kind — a multi-shape
mapping that switches realizations based on query shape (e.g., use a
pre-aggregated table when the query is a `groupBy` rollup, else use
the base table). This bridge does **not** support it.

```
ERR LEG003: mapping kind 'AggregationAware' is not supported.
            AggregationAware is a binding-resolution concern best
            handled at a higher layer (e.g., multiple class-mapping
            bindings with set IDs and a query-shape dispatch).
            Rewrite as multiple Relational bindings with explicit
            set IDs.
```

(`Relation`-kind is **supported**; see §5.5.5.)

### 6.3 Test suites referencing engine protocol-level metadata

`testSuites` with engine-specific JSON-shape assertions pass
through unchanged but won't execute under clean-sheet runtime
semantics. This is a test-harness concern, not a mapping concern.

---

## 7. Lint rule

Exactly one lint rule (`LEG001`) guards hand-written source for both
helpers — see §2.3 for the canonical statement.

**Where the lint runs:** at parse time on every `legacyNavigate` and
`legacyAssocPredicate` call whose AST node has `userSource` provenance.
The MappingNormalizer constructs both helpers programmatically — those
nodes carry `compilerSynthesized` provenance and never trigger the lint.

### Migration recipe (codemod-ready)

```pure
legacyNavigate(~slot: T, ($srcRow, $tgtRow) | $srcRow.COL_A == $tgtRow.COL_B)
```

becomes:

```pure
-- Step 1: synthesize +local on source binding
^Source(..., +colA = $srcRow.COL_A)

-- Step 2: synthesize +local on target binding (or use existing class property)
^Target(..., +colB = $tgtRow.COL_B)        -- or rely on existing $tgt.colB

-- Step 3: rewrite legacyNavigate → navigate with object predicate
navigate(~slot: T, (s, t) | $s.colA == $t.colB)
```

The codemod is a future tool; this doc specifies the *semantic*
rewrite.

---

## 8. Migration strategy

### Phase 0 — Today (coexistence)

- Parser handles both legacy DSL and clean-sheet syntax.
- MappingNormalizer translates legacy → Pure expressions.
- Clean-sheet mappings bypass the normalizer.
- Both forms coexist in user source freely.

### Phase 1 — Lint warnings on legacy DSL

- Parser emits `WARN LEG100: legacy mapping DSL detected` with a
  suggested clean-sheet rewrite per binding.

### Phase 2 — Codemod tool

- `legend-lite migrate-mappings <project>` rewrites legacy DSL to
  function form in place using this doc's semantic rules. Outputs a
  diff for review.

### Phase 3 — Legacy DSL deprecation

- Parser begins erroring on **new** legacy DSL (post-cutoff timestamp).

### Phase 4 — Removal

- Delete the MappingNormalizer's legacy-handling code.
- Delete `legacyNavigate` and `legacyAssocPredicate`.
- Parser rejects legacy DSL.
- One mapping shape end-to-end.

---

## 9. Open design questions

### 9.1 (resolved) AssociationMapping desugar uses `legacyAssocPredicate`, not `+local` synth

Resolved per §5.6.1 and §2.2: the AssociationMapping body is a
predicate function whose body uses the `legacyAssocPredicate` adapter,
paralleling the treatment of class-typed property mappings via
`legacyNavigate`. No `+local` synthesis at desugar time. Codemod is
responsible for lifting to object terms when migrating a property.

### 9.2 (resolved) Multi-hop uses pipeline `join` chain + final `legacyNavigate`, not `exists`

Resolved per §5.4.4: `legacyNavigate` is a pipeline step symmetric to
clean-sheet `navigate`. Multi-hop desugars to a chain of clean-sheet
`join` steps (one per intermediate physical row) terminated by a
single `legacyNavigate` step at the final hop to the target class. No
`exists` quantifier appears in the legacy bridge. The pipeline
structure itself binds intermediate rows as named slots.

### 9.3 Should we keep the name "MappingNormalizer"?

The phase is exclusively the legacy desugarer. A more honest name
would be `LegacyMappingDesugarer`. Renaming is a low-priority
cleanup; the existing name is fine as long as the doc and code
comments clarify its scope.

**Default:** keep the existing name for now to minimize churn.
Revisit in Phase 4 when the phase is being deleted anyway.

---

## 10. Summary

- **One phase, two parallel helpers.** `MappingNormalizer` translates
  legacy `MappingDefinition` → realizing Pure expressions. Two
  compiler-internal helpers cover the only shapes clean-sheet cannot
  express:
  - `legacyNavigate` — pipeline step, symmetric to clean-sheet
    `navigate`. Returns a class instance bound to a named slot.
  - `legacyAssocPredicate` — row-extraction adapter for
    AssociationMapping predicate function bodies. Returns Boolean.
- **All other legacy shapes** desugar to pure clean-sheet primitives:
  `tableReference`, `map`, `^Class`, `^Class[setId]`, `join`,
  `filter`, `distinct`, `groupBy`, `+local`,
  `otherwise`, positional cast for M2M, set-ID-qualified cast for
  enums, AssociationMapping predicate functions. (`~primaryKey` is parsed
  but not lowered — graph-fetch identity metadata, not a query op; §5.3.6.)
- **Includes** are resolved at parse time (binding-table
  flattening); store substitution is a Desugarer-internal token
  rewrite. No helper for either.
- **Multi-hop joins** use a pipeline `join` chain + final
  `legacyNavigate`. No `exists` quantifier in the bridge.
- **`Relation`-kind class mappings** are supported via §5.5.5 — they
  desugar trivially to `relationFn() -> filter -> map`.
- **Hard "no"s:** cross-store physical joins inside a class mapping,
  `AggregationAware`, and engine-protocol-coupled test suites.
- **Lint:** one rule (`LEG001`) guarding hand-written `legacyNavigate`
  and `legacyAssocPredicate`.
- **Hand-written clean-sheet mappings bypass the normalizer
  entirely** — their realizing functions are already Pure
  expressions.

The end-state: one mapping shape, end-to-end. The bridge keeps
legacy code running until codemod-assisted migration is complete.
