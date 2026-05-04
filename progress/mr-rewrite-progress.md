# MappingResolver rewrite — progress notes

> Living notes on the multi-phase MR rewrite. The end state is "MR is a
> single switch over typed HIR that produces resolved typed HIR; the
> sidecar `IdentityHashMap<TypedSpec, StoreResolution>` is gone; lowering
> reads everything off the AST."

## Done

### Phase 0b — parity fixture (`d07935d`)
SQL parity test (`MappingResolverParityTest`) covering 9 representative
queries. Acts as the byte-level safety net for every subsequent rewrite.

### Phase 1a — HirRewriter kernel extraction (`b80d8b8`)
Generic identity-preserving HIR rewriter extracted from `UserCallInliner`.
`Hooks` API gives strategies callbacks for the four substitution targets
(Variable, UserCall, Eval, GetAll); kernel handles all compound nodes.
α-renaming via `renameBinder` + `Scope.trim`.

### Phase 1b.i — alpha-rename utility (`41c2912`)
`HirRewriter.alphaRename(...)` for splice-time fresh-naming so spliced
synth bodies don't collide with user-query lambda params.

### Phase 1 — TypedGetAll splice (`96c78a0`)
`MappingResolver.inlineClassFetches` uses `HirRewriter` to splice the
compiled mapping function body in place of every `TypedGetAll`. After
this, no `TypedGetAll` survives in the rewritten HIR. Walk B (the old
`resolveMappingFunction`) still runs to populate the sidecar; Walk A
short-circuits on already-stamped synth nodes via a head pre-stamp
check.

### Phase 2 (read-side) — TypedPropertyAccess.physicalColumn field (`09ca578`)
`TypedPropertyAccess` gains `Optional<String> physicalColumn` with a
back-compat 4-arg ctor. `HirRewriter` rebuild preserves the field.
`PropertyAccessLowering` reads `pa.physicalColumn()` when present and
falls back to the sidecar's `columnFor` lookup otherwise.

### Phase 2 (write-side) — `PropertyAccessPopulator` (`7d7c00e`)
~470-line tree walker that mirrors `HirRewriter`'s switch. Threads a
`Map<String, StoreResolution>` env through lambda boundaries; at each
relop with lambdas (`TypedFilter`, `TypedExtend`, `TypedProject`,
`TypedSort`, `TypedFold`, `TypedMap`, `TypedJoin`/`TypedAsOfJoin`,
`TypedGroupBy`, `TypedAggregate`, `TypedPivot`) it binds lambda params
to the source's store. At each direct (no-path) `TypedPropertyAccess`,
calls `store.columnFor(prop)` and rewrites the access to carry the
resolved physical column on the AST. Wired in `MR.resolve()` step 3.

### Phase 2 (defensive) — `forwardStamp` on rebuild (`e27fc10`)
When the populator rebuilds a relop because a child changed, it now
forwards the original node's sidecar entry to the rebuilt instance.
Without this, schema-changing ops like `TypedProject` lose their
TDS-shaped output store on rebuild and `LoweringContext.storeFor`'s
source-recursion fallback returns the upstream class store instead.

## Verified

Every commit lands at exactly:
`Tests=2700 / Failures=69 / Errors=89 / Skipped=19` — `progress/baseline-failures.txt`
match. Parity 9/9 byte-identical.

## Attempted, reverted

### Phase 1d — structural extend-column pruning
Tried to drop unused `TypedExtendCol` entries from the AST during the
populator's walk (instead of leaving an `ExtendNodeCols` marker on the
sidecar for `ExtendLowering` to filter on). 33 regressions in
`AssociationIntegrationTest` / `M2MChainIntegrationTest`. Multiple
downstream consumers (graphFetch enumeration, `ExtendChecker`,
embedded-extend column emission) read association/embedded extend cols
off the AST, not the sidecar — structural pruning broke them.

To reattempt: enumerate every consumer of `TypedExtend.extensions()`
across the codebase; verify each tolerates a structurally-pruned list
(or get its info from the sidecar); only then drop cols.

## Pending

### Phase 1c — UserCallInliner unification
Refactor `MR.resolve()` to dispatch through a single `Hooks` impl
(`ResolverHooks`) wrapping `HirRewriter`, mirroring how
`UserCallInliner.inline()` does it. This is the architectural shift
that the rewrite plan is built around. Mostly a code-motion refactor
once the per-node logic is settled; defer until after Phase 3 because
Phase 3 will surface more per-node logic that should also live in the
hooks.

### Phase 3 — path-bearing TypedPropertyAccess → TypedJoin chains
The big payoff. Today path-bearing accesses (`$p.firm.name`) keep their
`associationPath` field and `PropertyAccessLowering` walks the path
through `selfStore.joins()` to install joins via `NavScope`. After
Phase 3, MR rewrites the access into a `TypedJoin` chain in the HIR;
the access becomes a direct `$firm_alias.name` on the join target;
`PropertyAccessLowering`'s entire path-walking loop deletes;
`JoinResolution` propagation in the sidecar disappears.

Implementation outline:
1. For each path-bearing `TypedPropertyAccess`, walk the source store's
   `joins` map hop by hop.
2. For each hop, emit a `TypedJoin` (or share an existing one with the
   same path prefix) — this requires lifting the join *above* the
   lambda's enclosing relational operator, which is structurally non-trivial.
3. Rewrite the access to point at the leaf join's alias.
4. Handle the four `JoinResolution` variants:
   - `FkJoin` → ordinary `TypedJoin`.
   - `StructArrayUnnest` → `TypedFlatten` + access on the unnested column.
   - `Embedded` → no join, just rewrite the access to use the parent alias
     with the embedded sub-column name.
   - `Otherwise` → unwrap to the fallback FK join (the embedded sub-col
     shortcut is already handled by the `Embedded` arm above).

Cost estimate: 2-3 days of focused work. Touches `MR`, `PropertyAccessLowering`,
`StoreResolution.JoinResolution`, possibly `NavScope`. Lots of test
churn — do it in a fresh branch with parity green at every step.

### Phase 4 — purge the sidecar's `propertyToColumn` consumers
Once Phase 3 lands, the only consumers of `StoreResolution.propertyToColumn`
are `JsonEnvelope`, `SortLimitLowering`, `ExtendLowering`,
`GroupByAggregateLowering`, `CollectionLowering`, `Relations`. Each
takes a property name and resolves a physical column. Replace with an
AST-side population pass analogous to `PropertyAccessPopulator`: every
operator that references a column by name carries the resolved physical
column on the AST.

### Phase 5 — delete the sidecar
Once nothing reads `IdentityHashMap<TypedSpec, StoreResolution>`,
delete it. `MR.resolve()` returns just the rewritten HIR.

## Open questions / risks

- **Lambda env ergonomics**: the populator uses a custom walker because
  `HirRewriter.Hooks` doesn't expose lambda binder context. If Phase 1c
  unifies on hooks, we'll need either a new hook API
  (`enterRelationalLambda(source, lam, scope)`) or thread store info
  through the existing `Scope`. Adding store info to `Scope` is the
  cleaner path because it's lambda-agnostic (lets, blocks, etc. all benefit).

- **Identity tracking through rebuilds**: `forwardStamp` is the current
  defense. Any new rewrite pass that rebuilds nodes must re-stamp.
  Worth promoting to a kernel feature: `Hooks.preserveStamp(original, rebuilt)`
  default-impl that forwards.

- **Structural pruning blocker**: at least 8 consumer files read
  association/embedded extend cols off the AST. Phase 1d depends on
  refactoring those to consume the sidecar (or computing the same info
  structurally elsewhere).
