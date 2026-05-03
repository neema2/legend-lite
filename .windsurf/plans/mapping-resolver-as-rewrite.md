# MappingResolver as a Tree Rewrite

## Problem

`MappingResolver` today is a **sidecar annotator** that stamps every typed-HIR
node with a `StoreResolution` record (table name, propertyToColumn map, joins
map, extend pruning marker). PlanGenerator/Lowering reads the sidecar via
`LoweringContext.storeFor(node)` and dispatches on resolved info.

This implementation has accreted three rewrite-in-disguise concerns expressed
as sidecar markers:

1. **`TypedGetAll` synth-body inlining.** The class fetch's compiled body is
   recursively walked at lowering time (`SourceLowering` and
   `Relations.joinTargetRelation`) instead of being inlined at MR time.
2. **`pruneUnusedExtendCols`.** Unused extend columns are *marked* with
   `ExtendNodeCols` instead of being dropped from the tree.
3. **Per-property column resolution.** `TypedPropertyAccess.property` carries a
   logical name; the physical column is recovered at lower-time via
   `store.columnFor(property)`.

The result: lowering does compilation work that MR should have done, the
sidecar's contract sprawls, and the boundary between "resolved" and "not yet
resolved" blurs.

## North Star

**MappingResolver is a logical→physical HIR rewrite pass.** Same shape as
`UserCallInliner` — and in fact, **the same pass**. Both are abstraction-
expansion: `TypedUserCall(fn, args)` and `TypedGetAll(class)` are
abstractions over a body that need their body spliced in with hygiene.
The end-state pipeline has **one inliner** that handles both.

`TypedSpec → TypedSpec`, no sidecar, substitution-based.

- **Input**: typed HIR with class fetches, logical property names, association
  navigations.
- **Output**: typed HIR with table refs, physical column names, explicit
  joins. Nothing logical remains.

### Worked example

User writes:

```pure
Person.all()->filter(p | $p.firstName == 'Alice')->project(p | $p.firm.legalName)
```

After MR:

```
TypedTableRef(T_PERSON) AS p0
  -> TypedFilter((p0) | p0.FIRST_NAME == 'Alice')
  -> TypedJoin(LEFT, TypedTableRef(T_FIRM) AS f0, ON p0.FIRM_ID = f0.ID)
  -> TypedProject((p0, f0) | f0.LEGAL_NAME)
```

Every name physical. Every join explicit. No `StoreResolution` anywhere.

## End-State Rules

The rewrite is defined by four substitution rules. They compose; the rewriter
is a `TypedSpec → TypedSpec` function applied bottom-up with a substitution
environment.

### Rule 1 — Class fetch inlining

`TypedGetAll(class)` rewrites to the inlined physical body of `class`'s
synthetic mapping function, with extend pruning applied.

- Memoized per FQN (one inlined-body per class per compilation unit).
- Cycle-guarded for self-joins (`shallowResolution` machinery preserved).
- M2M chains: the inlined body itself contains `TypedGetAll(upstream)` which
  the same rule expands recursively.
- Extend pruning during inlining:
  - Drop scalar/window/traverse extend cols not in
    `classPropertyAccesses[class]` (TypeChecker output).
  - Drop **all** association/embedded extend cols (their physical
    realization is added by Rule 3, not by synth body).

### Rule 2 — Logical property → physical column

`TypedPropertyAccess(row, "firstName")` where `row` is bound to a physical
table-ref alias rewrites to `TypedPropertyAccess(row, "FIRST_NAME")`.

Resolution uses the inlined body's PM seed: each physical row alias has a
known logical→physical mapping, computed once when its class was inlined,
threaded through the rewriter's substitution environment.

This is also where `forwardPassthrough` and `forwardRelationalRename` collapse:
they're not separate detectors, they're the substitution rule applied to
synth-body extend bodies during Rule 1's inlining (β-reduction substituting
`$row` bindings into `$row.prop` accesses).

### Rule 3 — Association navigation → explicit join

`TypedPropertyAccess(row, "leaf", associationPath=[a1, a2, ...])` rewrites by:

1. Walking `associationPath` from `row`'s class.
2. For each non-embedded hop, inserting a `TypedJoin(LEFT, target, ON cond)`
   immediately upstream of the smallest enclosing relational operator.
3. For each embedded hop, advancing the row context without inserting a join
   (sub-cols are on the parent's table).
4. For `Otherwise` hops, dispatching at rewrite time on whether `leaf` is in
   the embedded sub-col map: embedded path emits a parent column; FK fallback
   inserts a join.
5. For `StructArrayUnnest` hops, inserting a `TypedFlatten` (or new
   `TypedLateralUnnest`) node — the moral equivalent of a LEFT JOIN with
   `LATERAL UNNEST`.
6. Rewriting the access itself to `TypedPropertyAccess(joinedAlias, "PHYS_COL")`.

This is the today-NavScope algorithm at HIR level instead of MIR level. The
"smallest enclosing relational operator" is determined by tracking the
enclosing scope during the rewrite walk; same scoping discipline NavScope
uses today.

### Rule 4 — Implicit-serialize wrap

Class-typed roots get wrapped in `TypedSerializeImplicit` with a
`ResolvedGraphTree`. Each leaf carries `propertyName` (logical, for JSON
keys) + `physicalColumn` (for column projection).

- **Leaf list source**: the class's properties as declared in
  `modelContext.findClass(rootClass).properties()` — the model is the
  source-of-truth for "which fields serialize." Today's reliance on
  `rootStore.propertyToColumn().keySet()` is replaced.
- **Physical column for each leaf**: resolved against the rewritten root
  body's column schema (computed by Rule 1's inlining + Rule 2's
  resolution).
- Nested children (FK-joined sub-classes → correlated subqueries) are
  unchanged in semantics; the nested classes are also Rule-1-inlined and
  their `ResolvedGraphTree`s built recursively.

## Tree Conventions in End-State HIR

### Multi-param lambdas after joins

When MR inserts a join, downstream operators' lambdas become multi-param —
one param per in-scope alias. Example:

```
// Before MR: filter(p | $p.firm.legalName == 'X')
// After MR:
TypedTableRef(T_PERSON) AS p0
  -> TypedJoin(LEFT, TypedTableRef(T_FIRM) AS f0, ON p0.FIRM_ID = f0.ID)
  -> TypedFilter((p0, f0) | f0.LEGAL_NAME == 'X')
```

`TypedLambda.parameters` already supports multi-param (TypedJoin.condition is
binary today). Filter/Project/Sort/GroupBy lowering rules extend to bind each
param to its corresponding source alias.

### Aliases live in the typed HIR

`TypedTableRef` carries an alias name (`p0`, `f0`, ...). MR allocates aliases
during rewrite. Lowering uses them verbatim — `nextAlias()` for
relational-operator wrapping aliases stays, but anchor aliases come from the
tree. Eliminates "is this alias from the tree or from the lowerer's counter?"
ambiguity.

### Physical column names in `TypedPropertyAccess.property`

The field stays a `String`; after MR it holds the physical column name. No
new node type. Downstream (lowering, JSON envelope) emits the field verbatim.
For JSON envelope, the **logical** name comes from the `TypedGraphTree` leaf
(unchanged); the **physical** column comes from the resolved property access.

### What disappears

- `StoreResolution` record (entire file).
- `ResolvedMappings` record (entire file) and the `accessBindings` /
  `navigations` design slots (never populated; not needed).
- `ResolvedExpression` record — `MappingResolver.resolve()` returns
  `CompiledExpression` directly (with the rewritten HIR).
- `LoweringContext.storeFor()`, `Scalar.store` field on var bindings,
  `isRelationalSource()` (replaced by `n.info().type() instanceof Relation`).
- `NavScope`, `Relations.install`, `Relations.installJoin`,
  `Relations.installUnnest`, `Relations.joinTargetRelation` (joins are tree
  nodes; lowering is one-pass).
- `SourceLowering.lower(TypedGetAll)` — defensive throw (the node should not
  reach lowering).
- `Lowerer.SCALAR_WRAP_COLUMN` for class anchors — class anchors are
  `TypedTableRef`-rooted relations.
- `pruneUnusedExtendCols`, `ExtendNodeCols`, all four `columnFor` lowering
  callsites (5 calls total — SortLimitLowering's ternary calls twice), all
  three `propertyToColumn` direct accesses outside MR (CollectionLowering,
  JsonEnvelope.leafColumnRef, Relations.installUnnest) plus the one inside
  MR's `elaborateImplicitSerialize`.
- `forwardPassthrough`, `forwardRelationalRename` as separate methods —
  collapsed into Rule 1's substitution.
- `Walk B` (`resolveMappingFunction`): synth bodies are inlined, not stamped.
- `Walk A` (`resolveQuery` stamping): no sidecar to populate.

### What stays (renamed / repurposed)

- `resolveClassFetch(fqn)` — returns the inlined `TypedSpec` body (memoized).
- `resolveRelational`, `resolveM2M`, `resolveIdentity` — produce the inlined
  body's seed shape; their output is a `TypedSpec` subtree, not a record.
- `buildAssociationJoin`, `buildEmbeddedJoin`, `buildStructArrayJoins` —
  produce `TypedJoin`/`TypedFlatten` nodes (or fragments thereof) instead of
  `JoinResolution` records.
- `Otherwise` merge logic — at Rule 3, when expanding a hop, dispatch on
  embedded-sub-col-map vs FK fallback.
- Self-join cycle handling (`resolving` set, shallow fallback).
- `classPropertyAccesses` from TypeChecker — the input that drives Rule 1's
  pruning.
- Implicit-serialize wrap.

## Why This Is Achievable

The hard sub-problems and how each is bounded:

### H1: Multi-param lambdas after joins

TypedLambda already has `List<TypedParameter> parameters`. Filter/Project/
Sort/GroupBy lowering extends to bind each param to its alias. The number of
params at any point is determined by the join chain to the left. MR knows
this at rewrite time and emits the right number.

**Bound**: small change to ~5 lowering rules; lambda type unchanged.

### H2: Scoping discipline for join insertion

Rule 3 needs to find "the smallest enclosing relational operator" to host an
inserted join. NavScope solves this today at lowering time — the same
algorithm runs at HIR time:

- Each relational operator establishes a scope.
- Inside scalar lambda lowering, encountered association navigations are
  collected.
- At scope exit, collected joins are woven upstream of the operator's source.

The HIR rewriter implements this by passing a per-scope "pending joins" list
down into scalar contexts, and weaving them in when the scope's enclosing
operator is rebuilt.

**Bound**: pure algorithm port; no new compiler theory.

### H3: Otherwise dispatch at rewrite time

`Otherwise(embeddedSubCols, fallback)` requires deciding embedded vs FK at
each `$p.firm.<sub>` access. The decision is local: look up `<sub>` in
`embeddedSubCols`; if present, parent-column path; else FK path.

**Bound**: a `switch` inside Rule 3, identical logic to today's
`PropertyAccessLowering` Otherwise branch.

### H4: Struct-array unnest as a tree node

Today `JoinResolution.StructArrayUnnest` is a sidecar variant; lowering emits
a lateral-unnest join. In end state, MR emits a `TypedFlatten` node (or a new
`TypedLateralUnnest` if `TypedFlatten`'s semantics don't fit).

**Bound**: one node type; semantics already understood.

### H5: TypedNewInstance at relation root

Today `^Class(...)->...` literals get a `resolveIdentity` store. In end
state: MR rewrites these to a `TypedTdsLiteral` (or 1-row VALUES literal node
type) with property names as physical column names. Property accesses against
them lower as column refs uniformly.

**Bound**: one rewrite branch; no new infrastructure.

## Architecture: Unified Inliner

`UserCallInliner` and MR's class-fetch inlining are the **same operation**:
expand an abstraction over a body. The body-splice mechanic — deep-clone,
α-rename of bound HIR variables, capture-avoiding substitution of formals —
is identical. What differs is per-abstraction-kind work:

| | `TypedUserCall` | `TypedGetAll` |
|---|---|---|
| Body lookup | `CompiledFunction.body()` | synth body from `MappingNormalizer` |
| Formals → actuals | yes (function params) | no (synth body parameter-less) |
| Memoization | desirable, not present today | required (same class fetched many times) |
| Cycle handling | forbidden (no recursion) | required (self-joins) |
| Post-splice work | none | extend pruning, joins-map population |

**End-state shape**: a single `HirInliner` pass with a shared kernel and
two handlers. Runs to fixpoint — order-independent, since user functions
may contain `TypedGetAll` and inlined synth bodies are user-tree fragments
that may contain `TypedUserCall`s.

```
HirInliner.kernel:
  • substitute(env, body)       — capture-avoiding
  • alphaRename(lambda)         — fresh names for bound vars
  • freeNames(expr)
  • memo: Map<key, TypedSpec>   — shared across handlers

HirInliner.handlers:
  • TypedUserCall  → look up fn body, build env (formals→actuals),
                     splice via kernel, recurse
  • TypedGetAll    → look up synth body, splice via kernel (empty env),
                     prune unused extends, populate joins, recurse
```

Today's `UserCallInliner.java` is absorbed as the `TypedUserCall` handler.
MR's class-fetch logic becomes the `TypedGetAll` handler. Both pay into
the same memo and use the same hygiene machinery.

Phasing keeps existing tests green: Phase 1 builds the unified inliner
incrementally (extract kernel → add `TypedGetAll` handler → unify
top-level walker), with each commit shippable.

## Migration Phases

Each phase is shippable: the build stays green, all tests pass, the sidecar
shrinks monotonically. The end state is reached only at Phase 5; intermediate
phases keep the sidecar around but reduce its responsibilities.

### Phase 0 — Preparation (no behavior change)

- **TypedTableReference / TypedTdsLiteral stay alias-less**. SQL aliases are
  a lowering-output concept and do not belong at the HIR layer. After MR
  inlines a class-fetch body twice, the two copies are distinct Java
  objects, and `SourceLowering.lower(TypedTableReference)` allocates
  fresh aliases per-node via `ctx.nextAlias()` — no HIR-level alias
  field is needed. (MR's α-rename during inlining is for HIR-level
  **lambda parameters**, not SQL aliases — see Open Question 5.)
- **Graph tree phase-typing**: today's `TypedGraphTree` is a flat record
  `(propertyName, List<TypedGraphTree> children)`. Convert to a sealed
  interface with two variants:
  - `ParsedGraphTree(propertyName, children)` — pre-MR
  - `ResolvedGraphTree(propertyName, physicalColumn, children)` — post-MR

  MR rewrites parsed → resolved during Rule 4. **No nullable
  `Optional<String>` fields**: pre-MR and post-MR are different types, the
  compiler enforces phase invariants. JSON envelope code paths take
  `ResolvedGraphTree` only and read its non-null `physicalColumn`.
- **Decide TypedNewInstance / TypedCollection-of-classes rewrite target**:
  TypedTdsLiteral carries a parser-side `TdsLiteral` (cells as untyped
  values), which doesn't naturally fit a typed-HIR rewrite target. Phase 0
  introduces a `TypedClassValues(rows, columns, info)` record — a
  typed-cell N-row VALUES literal. (No alias field; lowering allocates
  per occurrence.)
- **Decide TypedLateralUnnest as new node**: today's `TypedFlatten` is
  `(source, column, def, info)` for the `flatten(~col)` Pure operator (one
  named column → multiple rows). It does NOT match struct-array
  lateral-unnest semantics. Phase 0 introduces a new
  `TypedLateralUnnest(source, arrayProperty, fields, info)` record. Rule
  3 / Phase 4 emits this node. (No alias field.)
- Identify tests that assert on sidecar internals
  (`storeResolutions.size()`, `StoreResolution` field reads). Each gets a
  per-phase migration:
  - Phases 1–2: assertions on `storeResolutions.size()` either survive
    (sidecar still populated for non-class nodes) or get replaced with a
    HIR-shape assertion (e.g., "no `TypedGetAll` past MR" replaces
    "X stores resolved"). Update in-place at the phase that breaks the
    assertion.
  - Phase 5: any remaining sidecar-content assertion is deleted; if the
    test was meaningful, replace with HIR-shape assertion (post-MR tree
    walk + structural check).
- **Build the parity fixture**: capture pre-Phase-1 SQL output for the
  ~20-query fixture set as snapshot files in
  `engine/src/test/resources/parity/`. These are the steady invariant;
  Phases 1–2 must not drift byte-by-byte; Phase 3 switches to normalized-
  alias semantic-equivalent assertion.

**Pin**: full test suite green, no semantic change.

### Phase 1 — Unified inliner with class-fetch handler

**Goal**: Rule 1 of the end state, in isolation. After this phase, no
`TypedGetAll` survives MR; pruning happens structurally; the inliner is
unified with `UserCallInliner`.

Sub-phasing (each is its own commit, tests green at every step):

#### Phase 1a — Extract substitution kernel from UserCallInliner

Pure refactor. Behavior unchanged.

- Pull substitution / α-rename / free-variable / capture-avoidance helpers
  out of `UserCallInliner.java` into a new `HirInliner` class with a
  `kernel` package-private API.
- `UserCallInliner.java` keeps its public entry point; internally
  delegates to `HirInliner.kernel.substitute(...)` etc.
- Add a `Memo<Key, TypedSpec>` to the kernel (initially empty — no caller
  uses it yet).
- All existing tests pass; no SQL drift; no semantic change.

#### Phase 1b — Class-fetch handler on the kernel

- New: `HirInliner.inlineClassFetch(classFqn) -> InlinedClass`. The
  result holds (a) the rewritten physical body as `TypedSpec` and (b) a
  `StoreResolution` carrying `tableName` + `propertyToColumn` + `joins`.
  Both are still consumed by Phases 1–2 lowering; Rule 3 in Phase 3 reads
  `joins`. Memoized via the shared kernel memo. Cycle-guarded.
- The inlining walk over a synth-body `TypedExtend`:
  - **Scalar/window/traverse extend cols**: kept iff alias ∈
    `classPropertyAccesses[class]`. Pruned cols don't appear in the
    output tree (no marker, no skip-flag).
  - **Association extend cols**: build the `JoinResolution` (today's
    `buildAssociationJoin`), attach to the class's
    `StoreResolution.joins`, **drop the col from the inlined tree**. The
    joins map remains the dispatch table consumed by NavScope at lowering
    time until Phase 3 deletes both.
  - **Embedded extend cols**: same as association — populate joins entry,
    drop from tree. Order-independent `Otherwise` merging preserved.
  - **`TypedExtend.traversalSpecs`** stays in the inlined tree as-is.
    These remain the eager-LEFT-JOIN mechanism until Phase 3 rewrites them
    into explicit `TypedJoin` nodes.
- M2M chain inlining: the rewriter recurses on inner `TypedGetAll(upstream)`
  and α-renames bound variables. `forwardPassthrough` and
  `forwardRelationalRename` collapse into β-substitution as the recursion
  substitutes `$row` against the upstream's resolved alias.
- **`TypedNewInstance` / `TypedCollection`-of-classes at relation root**:
  rewritten in this phase too — to a `TypedClassValues(rows, columns,
  alias, info)` literal (introduced in Phase 0) with property names as
  physical column names (identity-mapped).
- `resolveQuery` is replaced with the rewriter that, on `TypedGetAll`,
  splices in `inlineClassFetch(class).body()` (with fresh aliases via
  α-rename). User-query node stamping for non-class nodes still happens
  this phase (sidecar still consumed); Phase 5 deletes it.
- `SourceLowering.lower(TypedGetAll)` becomes
  `throw new IllegalStateException("TypedGetAll must not survive MR")`.
- `Relations.joinTargetRelation` deleted (no callers — class-fetch bodies
  are pre-inlined). `LoweringContext.compiledMappingFunction` deleted —
  both callers (`SourceLowering` for TypedGetAll, and `joinTargetRelation`)
  go away in Phase 1.
- `pruneUnusedExtendCols` and `ExtendNodeCols` are deleted. Lowering's
  `ExtendLowering` pruning branches are deleted.

#### Phase 1c — Unify the top-level walker

After 1a + 1b: `UserCallInliner` and the class-fetch handler share the
kernel but run as separate sequential passes. Phase 1c collapses them.

- `HirInliner.inline(unit)` runs both handlers to fixpoint in a single
  walker — `TypedUserCall` and `TypedGetAll` are dispatched in the same
  switch.
- `MappingResolver.resolve()` calls `HirInliner.inline(...)` once at the
  head; `UserCallInliner.inline(...)` is no longer called from
  `resolve()`'s prologue.
- `UserCallInliner.java` becomes a deprecated thin wrapper around
  `HirInliner` (or is deleted outright if no external callers remain —
  audit usages first).
- A user function whose body contains `Person.all()` now expands in one
  pass: the user-call handler splices the body, the walker continues
  into the spliced subtree, the class-fetch handler expands the
  `TypedGetAll`. No two-pass ordering dependency.

**Pin (1c)**: parity fixture byte-identical. No SQL drift. A test asserts
the rewritten HIR contains neither `TypedUserCall` nor `TypedGetAll`.

What still remains after Phase 1:
- The sidecar still exists for property→column lookup (Rule 2 not yet done).
  But the sidecar's storeResolutions map is now built by walking the
  rewritten tree, not the synth body — much smaller surface.
- Association joins still resolve via the sidecar `joins` map (Rule 3 not
  yet done).

**Pin**: a parity test that diffs SQL output before/after for a curated
fixtures set (relational mappings, M2M chains, embedded mappings, Otherwise
mappings, struct arrays). No SQL drift expected.

### Phase 2 — Per-property physical column rewrite (Rule 2)

**Goal**: every `TypedPropertyAccess` past MR carries a physical column name
in its `.property` field.

#### Mechanism: schema environment

The rewriter threads a `Map<TypedVariable, RowSchema>` through its walk,
where `RowSchema = LinkedHashMap<logicalProp, physicalCol>`. This is
Walk A's stamping work re-implemented as immediate threading instead of
node sidecars.

Per-operator schema-env update rules:

| Operator | Rule |
|---|---|
| `TypedTableRef(table, alias)` | Bind `alias → propToCol-from-PM-seed` (from inlined class's seed). |
| `TypedFilter(src, λ(row \| body))` | Lambda's `row` binds to src's schema. |
| `TypedSelect(src, cols)` | Result schema = src's restricted to `cols`. |
| `TypedRename(src, renames)` | Apply renames; renamed entries become `newName → newName`. |
| `TypedProject(src, cols)` | Result schema = `{col.alias → col.alias}` (identity / TDS-style). |
| `TypedGroupBy/Aggregate/Pivot` | Result schema = output aliases (identity). |
| `TypedExtend(src, extCols)` | Result schema = src's + each extend col's `alias → physicalCol`. User-query extend lambda bodies are walked here — their property accesses get rewritten too. |
| `TypedJoin(L, R, λ(lp, rp \| cond))` | Result has multi-alias schema; multi-param lambda binds each param to its side. |
| `TypedAsOfJoin`, `TypedConcatenate` | Result schema = left side's (matches today's inheritance). |
| `TypedIf(c, t, e)` | Both branches walked under same env; result = then-branch's. |
| `TypedBlock(stmts)` | Block's schema = last stmt's. |
| `TypedLet(name, value)` | Bind `name → value's schema` for subsequent stmts. |
| `TypedLambda` | Bind each declared param using outer scope. |

#### Property access rewrite

On `TypedPropertyAccess(row, "logical")` where `row` resolves to a known
schema in the env: rewrite to `TypedPropertyAccess(row, schema[logical])`.
If the access has a non-empty `associationPath`, leave it for Phase 3
(Phase 2 only resolves direct `$row.prop` accesses).

#### Cleanup at this phase

- Delete `StoreResolution.propertyToColumn`.
- Delete the 4 `columnFor()` callsites in lowering (5 calls —
  SortLimitLowering's ternary calls twice). Replace with direct
  `pa.property` read.
- Delete 3 direct `propertyToColumn()` accesses outside MR plus the one
  in MR's `elaborateImplicitSerialize`:
  - `MappingResolver.elaborateImplicitSerialize` — leaf list now from
    `modelContext.findClass(rootClass).properties()` (see Rule 4).
  - `CollectionLowering.buildClassValues` — column list comes from the
    rewritten `TypedClassValues` literal (Rule 1 already rewrote
    class-typed collections).
  - `JsonEnvelope.leafColumnRef` — reads `ResolvedGraphTree.physicalColumn`.
  - `Relations.installUnnest` — stays until Phase 4 (struct-array →
    `TypedLateralUnnest` tree node).
- `LoweringContext.Scalar.store` field dropped. `bindVar(name, expr)`
  signature drops the store argument.
- `JsonEnvelope.buildNestedSubquery`'s `bindVar` calls drop their store
  arg too — the nested join condition's property accesses are pre-resolved
  by Phase 2 (the nested class's body was inlined by Rule 1).

**Pin**: same parity test. No SQL drift.

### Phase 3 — Association navigation as `TypedJoin` (Rule 3)

**Goal**: every association traversal in user queries becomes a real
`TypedJoin` (or `TypedLateralUnnest`) in the rewritten HIR. After this
phase, the sidecar's `joins` map is unused.

Implementation:

- New: `MappingResolver.NavRewriter` — port of NavScope's algorithm to HIR
  level. Tracks per-scope pending joins; weaves them upstream of the
  enclosing relational operator at scope exit.
- `TypedPropertyAccess` with non-empty `associationPath` rewrites to a
  column reference on a join alias; the join(s) are inserted into the
  enclosing operator's source.
- **Multi-hop association extends**
  (`TypedAssociationExtendCol.hops()` with N>1): N hops emit N chained
  `TypedJoin` nodes, each binding its own alias. Today's
  `buildAssociationJoin` only used the last hop's condition; Phase 3
  walks all hops in chain.
- **Synth-body `TypedExtend.traversalSpecs`** (the `__hop_s_i` mechanism
  in `ExtendLowering` today): each hop in each spec emits a `TypedJoin`
  inserted upstream of the extend; the extend's lambda gains a param per
  spec terminal alias. Replaces NavScope's spec/hop loop.
- **Otherwise dispatch**: per-leaf-property check decides embedded vs FK.
- **Multi-param lambdas**: when a join is inserted, the enclosing
  relational operator's lambda gains the new alias as a parameter.
  Lowering rules updated to bind multi-param lambdas:
  - `FilterLowering`, `ProjectLowering`, `SortLimitLowering`,
    `GroupByAggregateLowering`, `ExtendLowering` (per-extend-col lambdas
    — every scalar/window/traverse extend body's lambda becomes
    multi-param when joins were inserted upstream).
  - `JoinLowering`'s `bindVar(...)` calls (already store-less from Phase 2)
    confirmed multi-param.
  - `TypedAsOfJoin.matchCondition` / `keyCondition` lambdas: same
    multi-param treatment when joins were inserted upstream of the
    AsOfJoin's source. (TypedConcatenate and TypedZip have no lambdas —
    nothing to update there; their sources' subtrees handle their own
    association navs.)
- `NavScope` deleted from `engine/src/main/java/com/gs/legend/plan/lowering`.
- `Relations.install`, `Relations.installJoin` deleted.
  `Relations.installUnnest` survives to Phase 4. `Relations.ensureAliased`
  stays (used for non-class subquery wrapping).
  `Relations.joinTargetRelation` and `LoweringContext.compiledMappingFunction`
  already deleted in Phase 1.

**Pin**: parity test. SQL output may differ in alias names but should be
semantically equivalent (same row sets, same column outputs, normalize-
able diff). Switch parity-fixture mode here from byte-identical to
normalized-alias.

### Phase 4 — Struct-array → `TypedLateralUnnest` (or reuse `TypedFlatten`)

**Goal**: inline struct-array properties (`Class[*]`) become tree nodes.
Sidecar `JoinResolution.StructArrayUnnest` deleted.

Implementation:

- Decide whether `TypedFlatten` semantics fit, or introduce a dedicated
  `TypedLateralUnnest` node carrying field list + parent alias.
- MR rewrites struct-array nav to insert the unnest node.
- `Relations.installUnnest` deleted.

**Pin**: parity test. Struct-array integration tests unchanged.

### Phase 5 — Cleanup

**Goal**: the sidecar disappears. Resolver returns `CompiledExpression`.

Implementation:

- Delete `StoreResolution.java`, `ResolvedMappings.java`,
  `ResolvedExpression.java`.
- `MappingResolver.resolve()` returns `CompiledExpression` (with rewritten
  HIR).
- Delete `LoweringContext.storeFor`, `LoweringContext.Scalar.store`,
  `isRelationalSource`'s store-based branch (replaced by
  `Type.Relation` check), `LoweringContext.compiledMappingFunction` (no
  callers after Phase 3).
- Delete remaining sidecar references in tests; replace assertions on
  `storeResolutions.size()` with assertions on the rewritten HIR's shape.
- Walk A (`resolveQuery`) and Walk B (`resolveMappingFunction`) entirely
  deleted; the rewriter is one walk.

**Pin**: parity test still green; sidecar grep returns zero matches in
`engine/src/main`.

## Test Strategy

- **Parity fixture**: a curated set of ~20 representative queries covering:
  pure relational mapping, M2M chain, embedded mapping, Otherwise mapping,
  self-join, struct-array, graph fetch (snapshot + streaming),
  filter/project/sort/groupBy/aggregate/extend, multi-hop association,
  TypedNewInstance literal, class collection. Each produces SQL output;
  capture a snapshot at Phase 0; assert byte-identical at Phases 1, 2 (alias
  names may shift at Phase 3 — switch to semantic-equivalent assertion
  there).
- **Existing integration tests** stay green throughout. Tests that assert on
  sidecar contents (e.g., `StressTest.storeResolutions().size()`) get
  updated phase-by-phase to assert on tree shape instead.
- **Per-phase smoke tests** added: e.g., Phase 1 adds a test that walks the
  rewritten HIR and asserts no `TypedGetAll` survives.

## Open Questions

1. **Resolved — SQL aliases stay in lowering**. SQL aliases are output-
   formatting detail, not HIR semantics. Lowering's per-node
   `ctx.nextAlias()` continues to allocate per-occurrence. After MR
   inlines a class-fetch body twice, the two copies are distinct nodes
   and lowering naturally allocates distinct aliases. No HIR record
   gets an alias field.

2. **Resolved — TypedTableReference and TypedTdsLiteral unchanged**.
   No structural change in Phase 0. They remain `(storeName, tableName,
   info)` and `(data, info)` respectively.

3. **Resolved — `TypedNewInstance` and `TypedCollection`-of-classes**:
   - **At relation root** (`^Person(...)->...` or
     `[^Person(a), ^Person(b)]->...`): MR rewrites to a `TypedClassValues`
     record (introduced Phase 0), carrying typed cells and the physical
     column list (property names = cols since identity-mapped). Property
     accesses against the rewritten literal are direct column refs. Done
     at Phase 1. (No alias on the record — lowering allocates.)
   - **In scalar position** (struct literal inside an extend body or as a
     function argument): no rewrite, stays as `TypedNewInstance`. Rules 1
     and 2 apply only to relation-root anchors and physical-row property
     accesses.

4. **Resolved — implicit-serialize tree leaf physical column**: TypedGraphTree
   becomes a sealed interface (Phase 0) with two variants —
   `ParsedGraphTree(propertyName, children)` and
   `ResolvedGraphTree(propertyName, physicalColumn, children)`. MR's
   Rule 4 builds the resolved variants from
   `modelContext.findClass(rootClass).properties()` (logical names) and
   the rewritten root body's column resolution (physical names).

5. **Resolved — memoization strategy**: eager template + α-rename per use
   site. `inlineClassFetch(fqn)` builds the complete inlined body once
   (including any nested class fetches in M2M chains, fully expanded).
   Per-use-site instantiation does a deep tree-clone with α-renaming of
   **bound HIR variables** (lambda parameters — `TypedLambda.params()`
   and their `TypedVariable` references) to fresh names. Crucially, this
   is HIR-variable renaming, NOT SQL-alias renaming. Lowering still
   allocates SQL aliases per-occurrence at lowering time. Sub-trees are
   shared by reference for unchanged sub-structure; duplication is
   bounded by chain depth × use sites, identical to today's
   lowering-time inlining.

## Risks

- **R1: Multi-param lambda dispatch in lowering** is the most invasive
  change to lowering. Mitigation: do it once in Phase 3, with
  comprehensive lambda-binding tests. **This is the cliff** — see
  "Architecture Decision Point" below.
- **R2: Alias allocation in HIR** breaks SQL-string snapshot tests.
  Mitigation: parity-fixture mechanism with normalized-alias diff at
  Phase 3+.
- **R3: Otherwise mapping dispatch at rewrite time** — verify the leaf-prop
  decision matches today's runtime dispatch in `PropertyAccessLowering`.
  Mitigation: dedicated Otherwise-mapping fixture in the parity test set.
- **~~R4: M2M chain inlining depth~~ — not actually a new risk.** Today's
  lowering already lowers each synth body fresh per use site
  (`Relations.joinTargetRelation` re-recurses), so the size growth exists
  today at MIR level. Phase 1 just makes it visible at HIR level. Java's
  reference-shared memoized sub-trees keep it bounded the same way today's
  lowering is.
- **R5: Cycle handling** — self-join associations and back-references need
  a stub inlined body. Phase 1 mitigation:
  - Cycle guard via existing `resolving: Set<String>` in `MappingResolver`.
  - Stub body shape: `TypedTableRef(table, freshAlias)` plus a
    `StoreResolution` seeded from the target class's PMs (logical→physical
    column map only, NO joins map). Matches today's `shallowResolution`
    exactly, just packaged as a body+store pair.
  - Schema env for the stub alias: PM-seed entries.
  - Behavior: a cycle target's row exposes its table's columns via
    PM-named properties; navigating further associations from the cycle
    target (Phase 3 only) would re-enter `resolveClassFetch` and either
    hit the memo (already resolved) or the cycle guard again (and stub).

## Implementation Principle: Mirror, Don't Invent

When implementing MR's per-operator rules (Phase 2's schema-env table,
Phase 3's join insertion), the existing 28 `bindVar(...)` callsites across
10 lowering files **are the spec**. Each MR rule must mirror what its
counterpart Lowering rule does today — same row binding, same source schema
inheritance, same scope discipline.

Concretely: when writing MR's rule for, say, `TypedExtend`, first read
`ExtendLowering.bindVar(...)` and the surrounding context in
`ExtendLowering.lower(...)`. Match its semantics. Do not invent new
binding rules.

This is the firewall against drift between MR's HIR rewrite and Lowerer's
MIR translation. If MR's schema-env disagrees with Lowerer's bindVar on
any operator, we have a bug — and the parity fixture will catch it
immediately as SQL drift.

**Rule of thumb**: every PR for Phases 2-3 cites the lowering file +
bindVar line numbers it mirrored. No PR adds an MR per-op rule without
this citation.

## Cleanup Order Across Phases

Walk B's three responsibilities migrate to different phases:

| Walk B job | Migrated by | Phase |
|---|---|---|
| Stamp synth-body nodes | Synth body becomes part of user tree, stamped by Walk A | 1 |
| Build `propertyToColumn` | Rule 1 inlining seeds it; Rule 2 makes it irrelevant | 1 (build), 2 (delete) |
| Build `joins` | Rule 1 inlining still populates; Rule 3 makes it irrelevant | 1 (build), 3 (delete) |

`Relations.*` cleanup order:

| Method | Deleted at |
|---|---|
| `Relations.joinTargetRelation` | Phase 1 (no callers — class fetches pre-inlined) |
| `Relations.install`, `Relations.installJoin` | Phase 3 (NavScope gone, joins are tree nodes) |
| `Relations.installUnnest` | Phase 4 (struct-array unnest is a tree node) |
| `Relations.ensureAliased` | Stays — used for non-class subquery wrapping |

`LoweringContext` field cleanup order:

| Field/method | Deleted at |
|---|---|
| `compiledMappingFunction(class)` | Phase 1 (callers: `SourceLowering(TypedGetAll)`, `Relations.joinTargetRelation` — both deleted) |
| `Scalar.store` field on var binding | Phase 2 |
| `bindVar(name, expr, store)` store arg | Phase 2 (signature change) |
| `navScope`, `withNavScope` | Phase 3 |
| `storeFor(node)` | Phase 5 |
| `mappings()` (sidecar accessor) | Phase 5 |
| `aliases()` (alias supplier) | Stays — lowering still allocates subquery aliases |

## Architecture Decision Point: Phase 3

Phases 1+2 are commit-worthy on their own merits and reduce the sidecar's
responsibilities without changing the architecture. After Phase 2:

- No `TypedGetAll` past MR.
- No `propertyToColumn` map.
- No `ExtendNodeCols` marker, no Walk B mid-walk mutation.
- 5 `columnFor()` lowering callsites + 4 direct `propertyToColumn` accesses
  deleted.
- The sidecar still exists for `joins` dispatch (legitimate sealed-variant
  resolved table). `NavScope`, `Relations.install*` still exist at
  lowering time.

This is a **defensible intermediate end state**: "two stages with a small
resolved-dict bridge for joins." The mental model is clear: MR resolves
type + property + class-fetch; lowering does translation + association nav
dispatch. Cleaner than today, less invasive than the full plan.

Phase 3 is the architecture decision: do we go all the way to
"logical→physical HIR rewrite" with associations as `TypedJoin` nodes? It
requires:

- Multi-param lambda dispatch in 5 lowering rules (Filter, Project, Sort,
  GroupBy, Aggregate).
- Porting NavScope's algorithm to HIR level.
- Alias allocation moves into MR.
- Lowering snapshot tests need normalized-alias mode.

**Decide whether to do Phase 3 AFTER Phases 1+2 ship.** The intermediate
end state will tell us how the codebase feels. If new features keep wanting
the sidecar to grow, Phase 3 pays off. If the joins-dict feels stable,
stopping here is honest.

## Success Criteria

- `MappingResolver.resolve()` returns `CompiledExpression`; no
  `ResolvedExpression` / `ResolvedMappings` / `StoreResolution` exist in
  `main/java`.
- `LoweringContext` has no `storeFor`, `mappings`, or store-bearing
  bindings.
- `TypedGetAll` does not appear in any post-MR HIR.
- Every `TypedPropertyAccess` past MR has a physical column in `.property`.
- Every association traversal past MR is an explicit `TypedJoin` or
  `TypedLateralUnnest` node.
- All existing integration tests pass; SQL output is semantically
  equivalent (byte-identical where alias allocation hasn't shifted).
- Sidecar grep on `engine/src/main`: zero matches for `StoreResolution`,
  `ResolvedMappings`, `storeFor`.
